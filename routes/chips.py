# routes/chips.py
# -*- coding: utf-8 -*-

import re
import uuid

from flask import Blueprint, jsonify, redirect, render_template, request, url_for, flash
from google.cloud import bigquery

from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()
PROJECT = bq.project
DATASET = bq.dataset


def only_digits(value):
    return re.sub(r"\D+", "", str(value or ""))


def clean_text(value, default=None):
    if value is None:
        return default
    value = str(value).strip()
    return value if value else default


def to_int(value, default=None):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except Exception:
        return default


def to_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", "."))
    except Exception:
        return default


def param(name, type_, value):
    return bigquery.ScalarQueryParameter(name, type_, value)


def run_op(operation, sql, params=None):
    print(f"🔵 BigQuery operação={operation} projeto={PROJECT} dataset={DATASET}")
    try:
        return bq.run(sql, params=params)
    except Exception as exc:
        print(f"🚨 Erro BigQuery operação={operation}: {exc}")
        raise


def fetch_one(sql, params=None):
    df = bq.run_df(sql, params=params)
    return None if df.empty else sanitize_df(df).iloc[0].to_dict()


def chip_by_clean_number(numero_limpo, exclude_sk=None):
    where = "REGEXP_REPLACE(numero, r'[^0-9]', '') = @numero_limpo AND COALESCE(ativo, TRUE) = TRUE"
    params = [param("numero_limpo", "STRING", numero_limpo)]
    if exclude_sk:
        where += " AND sk_chip != @exclude_sk"
        params.append(param("exclude_sk", "INT64", int(exclude_sk)))
    return fetch_one(f"""
        SELECT sk_chip, id_chip, numero, status
        FROM `{PROJECT}.{DATASET}.dim_chip`
        WHERE {where}
        LIMIT 1
    """, params)


def insert_event(sk_chip, tipo, observacao, origem="Painel"):
    try:
        run_op("registrar evento", f"""
            INSERT INTO `{PROJECT}.{DATASET}.f_chip_evento`
                (sk_chip, tipo_evento, origem, observacao, created_at)
            VALUES (@sk_chip, @tipo, @origem, @observacao, CURRENT_TIMESTAMP())
        """, [param("sk_chip", "INT64", int(sk_chip)), param("tipo", "STRING", tipo), param("origem", "STRING", origem), param("observacao", "STRING", observacao)])
    except Exception as exc:
        print(f"⚠️ Evento não registrado ({tipo}) para sk_chip={sk_chip}: {exc}")


@chips_bp.route("/chips")
def chips_list():
    try:
        page = max(to_int(request.args.get("page"), 1), 1)
        per_page = min(max(to_int(request.args.get("per_page"), 50), 10), 100)
        offset = (page - 1) * per_page
        filters = {k: clean_text(request.args.get(k)) for k in ["q", "status", "operadora", "plano", "operador", "aparelho", "tipo_whatsapp", "ativo", "quick"]}
        where, params = ["1=1"], []
        if filters["q"]:
            where.append("(REGEXP_REPLACE(COALESCE(numero,''), r'[^0-9]', '') LIKE @q_digits OR LOWER(COALESCE(numero,'')) LIKE @q OR LOWER(COALESCE(id_chip,'')) LIKE @q)")
            params += [param("q", "STRING", f"%{filters['q'].lower()}%"), param("q_digits", "STRING", f"%{only_digits(filters['q'])}%")]
        for field in ["status", "operadora", "plano", "operador", "tipo_whatsapp"]:
            if filters[field]:
                where.append(f"COALESCE(CAST({field} AS STRING),'') = @{field}")
                params.append(param(field, "STRING", filters[field]))
        if filters["aparelho"]:
            where.append("CAST(sk_aparelho_atual AS STRING) = @aparelho")
            params.append(param("aparelho", "STRING", filters["aparelho"]))
        if filters["ativo"] in ["true", "false"]:
            where.append("COALESCE(ativo, TRUE) = @ativo")
            params.append(param("ativo", "BOOL", filters["ativo"] == "true"))
        quick = filters["quick"]
        if quick == "sem_aparelho": where.append("sk_aparelho_atual IS NULL")
        if quick == "banidos": where.append("UPPER(COALESCE(status,'')) = 'BANIDO'")
        if quick == "recarga": where.append("(ultima_recarga_data IS NULL OR ultima_recarga_data < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))")
        select_cols = "sk_chip,id_chip,numero,operadora,plano,status,ultima_recarga_valor,ultima_recarga_data,total_gasto,sk_aparelho_atual,ativo,updated_at,operador,observacao,slot_whatsapp,tipo_whatsapp,data_status,dt_inicio,qt_banimentos,dt_banimentos,qt_disparos"
        sql = f"""
            SELECT {select_cols}, COUNT(*) OVER() AS total_count
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            WHERE {' AND '.join(where)}
            ORDER BY
              CASE WHEN UPPER(COALESCE(status,'')) IN ('BANIDO','BLOQUEADO','RESTRINGIDO','MANUTENCAO') THEN 0 ELSE 1 END,
              CASE WHEN sk_aparelho_atual IS NULL THEN 0 ELSE 1 END,
              CASE WHEN ultima_recarga_data IS NULL OR ultima_recarga_data < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 0 ELSE 1 END,
              CASE WHEN UPPER(COALESCE(status,'')) IN ('ATIVO','EM_USO') THEN 0 ELSE 1 END,
              updated_at DESC
            LIMIT @limit OFFSET @offset
        """
        params += [param("limit", "INT64", per_page), param("offset", "INT64", offset)]
        chips_df = sanitize_df(bq.run_df(sql, params=params))
        total = int(chips_df["total_count"].iloc[0]) if not chips_df.empty and "total_count" in chips_df else 0
        aparelhos_df = sanitize_df(bq.run_df(f"SELECT sk_aparelho, marca, modelo FROM `{PROJECT}.{DATASET}.dim_aparelho` ORDER BY marca, modelo LIMIT 500"))
        chips_records = chips_df.drop(columns=["total_count"], errors="ignore").to_dict("records")
        stats = {
            "ativos": sum(1 for c in chips_records if c.get("ativo") is True),
            "banidos": sum(1 for c in chips_records if c.get("status") == "BANIDO"),
            "disponiveis": sum(1 for c in chips_records if c.get("status") == "DISPONIVEL"),
            "sem_aparelho": sum(1 for c in chips_records if not c.get("sk_aparelho_atual")),
            "vinculados": sum(1 for c in chips_records if c.get("sk_aparelho_atual")),
            "precisam_recarga": sum(1 for c in chips_records if not c.get("ultima_recarga_data")),
            "total_gasto": sum(float(c.get("total_gasto") or 0) for c in chips_records),
            "media_disparos": round(sum(float(c.get("qt_disparos") or 0) for c in chips_records) / max(len(chips_records), 1), 1),
            "total_banimentos": sum(int(c.get("qt_banimentos") or 0) for c in chips_records),
            "whatsapp_normal": sum(1 for c in chips_records if c.get("tipo_whatsapp") == "NORMAL"),
            "whatsapp_business": sum(1 for c in chips_records if c.get("tipo_whatsapp") == "BUSINESS"),
        }
        return render_template("chips.html", chips=chips_records, aparelhos=aparelhos_df.to_dict("records"), page=page, per_page=per_page, total=total, filters=filters, stats=stats)
    except Exception as e:
        print("🚨 Erro ao listar chips:", e)
        return render_template("chips.html", chips=[], aparelhos=[], page=1, per_page=50, total=0, filters={}, stats={}, error="Erro ao carregar listagem de chips."), 200


@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    data = request.form.to_dict()
    numero_limpo = only_digits(data.get("numero"))
    operadora = clean_text(data.get("operadora"))
    if not numero_limpo or not operadora:
        flash("Número e operadora são obrigatórios.", "error"); return redirect(url_for("chips.chips_list"))
    try:
        dup = chip_by_clean_number(numero_limpo)
        if dup:
            flash(f"Chip já cadastrado para este número (SK {dup['sk_chip']}).", "error"); return redirect(url_for("chips.chips_list"))
        id_chip = clean_text(data.get("id_chip"), f"CHIP-{uuid.uuid4().hex[:10].upper()}")
        status = clean_text(data.get("status"), "DISPONIVEL")
        observacao = clean_text(data.get("observacao"), "")
        run_op("cadastrar chip", f"CALL `{PROJECT}.{DATASET}.sp_insert_chip`(@p_id_chip,@p_numero,@p_operadora,@p_plano,@p_status,@p_observacao,@p_origem)", [param("p_id_chip", "STRING", id_chip), param("p_numero", "STRING", numero_limpo), param("p_operadora", "STRING", operadora), param("p_plano", "STRING", clean_text(data.get("plano"), "")), param("p_status", "STRING", status), param("p_observacao", "STRING", observacao), param("p_origem", "STRING", "Painel")])
        chip = chip_by_clean_number(numero_limpo)
        if not chip or not chip.get("sk_chip"):
            raise RuntimeError("Chip inserido, mas sk_chip não foi encontrado na dim_chip.")
        sk_chip = int(chip["sk_chip"])
        updates = []
        params = [param("sk_chip", "INT64", sk_chip)]
        for field, typ, default in [("operador","STRING",None),("tipo_whatsapp","STRING",None),("slot_whatsapp","INT64",None),("qt_disparos","INT64",0),("qt_banimentos","INT64",0),("dt_banimentos","DATE",None)]:
            val = data.get(field)
            if field.startswith("qt_"): val = to_int(val, default)
            elif field == "slot_whatsapp": val = to_int(val)
            elif field == "dt_banimentos": val = clean_text(val)
            else: val = clean_text(val)
            updates.append(f"{field}=@{field}"); params.append(param(field, typ, val))
        updates.append("ativo=TRUE"); updates.append("total_gasto=COALESCE(total_gasto,0)"); updates.append("updated_at=CURRENT_TIMESTAMP()")
        run_op("complementar cadastro", f"UPDATE `{PROJECT}.{DATASET}.dim_chip` SET {', '.join(updates)} WHERE sk_chip=@sk_chip", params)
        if to_int(data.get("sk_aparelho_atual")):
            _vincular_chip(sk_chip, to_int(data.get("sk_aparelho_atual")), to_int(data.get("slot_whatsapp")), clean_text(data.get("tipo_whatsapp")))
        insert_event(sk_chip, "CADASTRO", "Cadastro realizado pelo painel")
        flash(f"Chip cadastrado com sucesso (SK {sk_chip}).", "success")
    except Exception as e:
        print(f"🚨 Erro ao cadastrar chip numero={numero_limpo}: {e}")
        flash(f"Erro ao cadastrar chip: {e}", "error")
    return redirect(url_for("chips.chips_list"))


def _vincular_chip(sk_chip, sk_aparelho, slot, tipo_whatsapp=None):
    if not slot: raise ValueError("Slot WhatsApp é obrigatório para vincular aparelho.")
    dup = fetch_one(f"SELECT sk_chip FROM `{PROJECT}.{DATASET}.dim_chip` WHERE sk_aparelho_atual=@ap AND slot_whatsapp=@slot AND sk_chip!=@sk AND COALESCE(ativo,TRUE)=TRUE LIMIT 1", [param("ap","INT64",sk_aparelho), param("slot","INT64",slot), param("sk","INT64",sk_chip)])
    if dup: raise ValueError("Este slot já está ocupado no aparelho selecionado.")
    run_op("vincular aparelho", f"UPDATE `{PROJECT}.{DATASET}.dim_chip` SET sk_aparelho_atual=@ap, slot_whatsapp=@slot, tipo_whatsapp=@tipo, updated_at=CURRENT_TIMESTAMP() WHERE sk_chip=@sk", [param("ap","INT64",sk_aparelho), param("slot","INT64",slot), param("tipo","STRING",tipo_whatsapp), param("sk","INT64",sk_chip)])
    insert_event(sk_chip, "VINCULO_APARELHO", f"Vinculado ao aparelho {sk_aparelho}, slot {slot}")


@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    try:
        row = fetch_one(f"SELECT sk_chip,id_chip,numero,operadora,plano,status,operador,observacao,tipo_whatsapp,slot_whatsapp,qt_disparos,qt_banimentos,dt_banimentos,data_status,dt_inicio,sk_aparelho_atual,ultima_recarga_valor,ultima_recarga_data,total_gasto FROM `{PROJECT}.{DATASET}.vw_chips_painel` WHERE sk_chip=@sk LIMIT 1", [param("sk","INT64",sk_chip)])
        return (jsonify(row), 200) if row else (jsonify({"error":"Chip não encontrado"}),404)
    except Exception as e:
        print("🚨 Erro ao buscar chip:", e); return jsonify({"error":"Erro interno ao buscar chip"}),500


@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        p = request.json or {}; sk = to_int(p.get("sk_chip"))
        if not sk: return jsonify({"error":"sk_chip ausente"}),400
        atual = fetch_one(f"""
            SELECT sk_chip, numero, operadora, plano, status, operador, observacao, tipo_whatsapp,
                   slot_whatsapp, qt_disparos, qt_banimentos, dt_banimentos, data_status, sk_aparelho_atual
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip=@sk
            LIMIT 1
        """, [param("sk","INT64",sk)])
        if not atual: return jsonify({"error":"Chip não encontrado"}),404
        numero_limpo = only_digits(p.get("numero"))
        if not numero_limpo: return jsonify({"error":"Número obrigatório"}),400
        dup = chip_by_clean_number(numero_limpo, exclude_sk=sk)
        if dup: return jsonify({"error":f"Já existe chip ativo com este número (SK {dup['sk_chip']})."}),409
        status_novo = clean_text(p.get("status"), atual.get("status") or "DISPONIVEL")
        set_data_status = "data_status = CURRENT_DATE()," if status_novo != atual.get("status") else "data_status = @data_status,"
        run_op("editar chip", f"""
            UPDATE `{PROJECT}.{DATASET}.dim_chip` SET
              numero=@numero, operadora=@operadora, plano=@plano, status=@status, operador=@operador,
              observacao=@observacao, tipo_whatsapp=@tipo_whatsapp, slot_whatsapp=@slot_whatsapp,
              qt_disparos=@qt_disparos, qt_banimentos=@qt_banimentos, dt_banimentos=@dt_banimentos,
              {set_data_status} updated_at=CURRENT_TIMESTAMP()
            WHERE sk_chip=@sk
        """, [param("numero","STRING",numero_limpo), param("operadora","STRING",clean_text(p.get("operadora"))), param("plano","STRING",clean_text(p.get("plano"))), param("status","STRING",status_novo), param("operador","STRING",clean_text(p.get("operador"))), param("observacao","STRING",clean_text(p.get("observacao"))), param("tipo_whatsapp","STRING",clean_text(p.get("tipo_whatsapp"))), param("slot_whatsapp","INT64",to_int(p.get("slot_whatsapp"))), param("qt_disparos","INT64",to_int(p.get("qt_disparos"),0)), param("qt_banimentos","INT64",to_int(p.get("qt_banimentos"),0)), param("dt_banimentos","DATE",clean_text(p.get("dt_banimentos"))), param("data_status","DATE",clean_text(p.get("data_status") or p.get("dt_inicio"))), param("sk","INT64",sk)])
        if "sk_aparelho_atual" in p:
            ap = to_int(p.get("sk_aparelho_atual"))
            if ap: _vincular_chip(sk, ap, to_int(p.get("slot_whatsapp")), clean_text(p.get("tipo_whatsapp")))
            else:
                run_op("desvincular aparelho", f"UPDATE `{PROJECT}.{DATASET}.dim_chip` SET sk_aparelho_atual=NULL, slot_whatsapp=NULL, tipo_whatsapp=NULL, updated_at=CURRENT_TIMESTAMP() WHERE sk_chip=@sk", [param("sk","INT64",sk)])
                insert_event(sk, "DESVINCULO_APARELHO", "Desvinculado pelo painel")
        insert_event(sk, "EDICAO", "Chip editado pelo painel")
        return jsonify({"success": True})
    except Exception as e:
        print("🚨 Erro ao editar chip:", e); return jsonify({"error": str(e)}),500


@chips_bp.route("/chips/recarga", methods=["POST"])
def chips_recarga():
    try:
        p = request.json or {}; sk = to_int(p.get("sk_chip")); valor = to_float(p.get("valor"))
        if not sk or valor is None: return jsonify({"error":"sk_chip e valor obrigatórios"}),400
        try:
            run_op("registrar recarga SP", f"CALL `{PROJECT}.{DATASET}.sp_registrar_recarga_chip`(@sk,@valor,@origem,@obs)", [param("sk","INT64",sk), param("valor","FLOAT64",valor), param("origem","STRING","Painel"), param("obs","STRING",clean_text(p.get("observacao"),"Recarga via painel"))])
        except Exception:
            run_op("registrar recarga update", f"UPDATE `{PROJECT}.{DATASET}.dim_chip` SET ultima_recarga_valor=@valor, ultima_recarga_data=CURRENT_DATE(), total_gasto=COALESCE(total_gasto,0)+@valor, updated_at=CURRENT_TIMESTAMP() WHERE sk_chip=@sk", [param("valor","FLOAT64",valor), param("sk","INT64",sk)])
        insert_event(sk, "RECARGA", f"Recarga de R$ {valor:.2f}")
        return jsonify({"success": True})
    except Exception as e:
        print("🚨 Erro ao recarregar chip:", e); return jsonify({"error": str(e)}),500


@chips_bp.route("/chips/banir", methods=["POST"])
def chips_banir():
    try:
        sk = to_int((request.json or {}).get("sk_chip"))
        if not sk: return jsonify({"error":"sk_chip obrigatório"}),400
        run_op("banir chip", f"UPDATE `{PROJECT}.{DATASET}.dim_chip` SET status='BANIDO', qt_banimentos=COALESCE(qt_banimentos,0)+1, dt_banimentos=CURRENT_DATE(), data_status=CURRENT_DATE(), updated_at=CURRENT_TIMESTAMP() WHERE sk_chip=@sk", [param("sk","INT64",sk)])
        insert_event(sk, "BANIMENTO", "Banimento registrado pelo painel")
        return jsonify({"success": True})
    except Exception as e:
        print("🚨 Erro ao banir chip:", e); return jsonify({"error":str(e)}),500


@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    try:
        df = bq.run_df(f"""
            SELECT sk_chip, tipo_evento, origem, observacao, created_at AS data_evento
            FROM `{PROJECT}.{DATASET}.f_chip_evento`
            WHERE sk_chip=@sk
            ORDER BY created_at DESC
            LIMIT 100
        """, [param("sk","INT64",sk_chip)])
        return jsonify(sanitize_df(df).to_dict("records"))
    except Exception as e:
        print("🚨 Erro timeline:", e); return jsonify([]),500


@chips_bp.route("/admin/diagnostico")
def diagnostico():
    checks = []
    queries = {
        "dim_chip existe": f"SELECT 1 FROM `{PROJECT}.{DATASET}.dim_chip` LIMIT 1",
        "vw_chips_painel existe": f"SELECT 1 FROM `{PROJECT}.{DATASET}.vw_chips_painel` LIMIT 1",
        "chips sem sk_chip": f"SELECT COUNT(*) qtd FROM `{PROJECT}.{DATASET}.dim_chip` WHERE sk_chip IS NULL",
        "números duplicados ativos": f"SELECT COUNT(*) qtd FROM (SELECT REGEXP_REPLACE(numero, r'[^0-9]', '') n FROM `{PROJECT}.{DATASET}.dim_chip` WHERE COALESCE(ativo,TRUE) GROUP BY n HAVING COUNT(*)>1)",
        "vínculo inválido": f"SELECT COUNT(*) qtd FROM `{PROJECT}.{DATASET}.dim_chip` c LEFT JOIN `{PROJECT}.{DATASET}.dim_aparelho` a ON c.sk_aparelho_atual=a.sk_aparelho WHERE c.sk_aparelho_atual IS NOT NULL AND a.sk_aparelho IS NULL",
        "slots duplicados": f"SELECT COUNT(*) qtd FROM (SELECT sk_aparelho_atual, slot_whatsapp FROM `{PROJECT}.{DATASET}.dim_chip` WHERE sk_aparelho_atual IS NOT NULL AND slot_whatsapp IS NOT NULL GROUP BY 1,2 HAVING COUNT(*)>1)",
    }
    for nome, sql in queries.items():
        try:
            row = fetch_one(sql); checks.append({"check": nome, "ok": True, "resultado": row})
        except Exception as e:
            checks.append({"check": nome, "ok": False, "erro": str(e)})
    return jsonify(checks)
