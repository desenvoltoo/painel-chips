# routes/chips.py
# -*- coding: utf-8 -*-

import re
import uuid
import time

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


def get_table_columns(table_name):
    try:
        df = bq.run_df(f"""
            SELECT column_name
            FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = @table_name
        """, [param("table_name", "STRING", table_name)])
        return {str(row["column_name"]) for _, row in df.iterrows()}
    except Exception as exc:
        print(f"[Chips] Erro ao ler schema de {table_name}: {exc}")
        return set()


def pick_col(columns, *names):
    for name in names:
        if name in columns:
            return name
    return None


def chip_select_expr(columns, aliases=False):
    def expr(alias, typ="STRING", *candidates, default=None):
        col = pick_col(columns, *candidates, alias)
        if col:
            base = f"CAST({col} AS {typ})" if typ else col
        elif default is not None:
            base = default
        else:
            base = f"CAST(NULL AS {typ})" if typ else "NULL"
        return f"{base} AS {alias}" if aliases else base

    return {
        "sk_chip": expr("sk_chip", "INT64", "sk_chip", "chip_id", "id", "id_chip"),
        "id_chip": expr("id_chip", "STRING", "id_chip", "chip_id", "id"),
        "numero": expr("numero", "STRING", "numero", "linha", "telefone", "msisdn"),
        "operadora": expr("operadora", "STRING", "operadora", "carrier"),
        "plano": expr("plano", "STRING", "plano"),
        "status": expr("status", "STRING", "status", "situacao", default="'SEM_STATUS'"),
        "ultima_recarga_valor": expr("ultima_recarga_valor", "FLOAT64", "ultima_recarga_valor"),
        "ultima_recarga_data": expr("ultima_recarga_data", "DATE", "ultima_recarga_data"),
        "total_gasto": expr("total_gasto", "FLOAT64", "total_gasto"),
        "sk_aparelho_atual": expr("sk_aparelho_atual", "INT64", "sk_aparelho_atual", "aparelho_id"),
        "ativo": expr("ativo", "BOOL", "ativo", default="TRUE"),
        "updated_at": expr("updated_at", "TIMESTAMP", "updated_at", "data_update", "updatedAt"),
        "created_at": expr("created_at", "TIMESTAMP", "created_at", "data_cadastro", "createdAt", "data_update", "updated_at"),
        "operador": expr("operador", "STRING", "operador", "responsavel", "usuario_nome", "usuario", "responsavel_nome"),
        "observacao": expr("observacao", "STRING", "observacao", "obs"),
        "slot_whatsapp": expr("slot_whatsapp", "INT64", "slot_whatsapp"),
        "tipo_whatsapp": expr("tipo_whatsapp", "STRING", "tipo_whatsapp"),
        "data_status": expr("data_status", "DATE", "data_status"),
        "dt_inicio": expr("dt_inicio", "DATE", "dt_inicio"),
        "qt_banimentos": expr("qt_banimentos", "INT64", "qt_banimentos", default="0"),
        "dt_banimentos": expr("dt_banimentos", "DATE", "dt_banimentos"),
        "qt_disparos": expr("qt_disparos", "INT64", "qt_disparos", default="0"),
    }


def chip_select_list(columns):
    return ",".join(chip_select_expr(columns, aliases=True).values())


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
    started = time.perf_counter()
    route_params = {k: v for k, v in request.args.items() if clean_text(v) is not None}
    print(f"[Chips] Rota /chips chamada. Params={route_params}")
    try:
        page = max(to_int(request.args.get("page"), 1), 1)
        per_page = min(max(to_int(request.args.get("per_page"), 50), 10), 100)
        offset = (page - 1) * per_page
        filter_names = [
            "q", "status", "operadora", "plano", "operador", "responsavel", "aparelho",
            "tipo_whatsapp", "ativo", "quick", "created_from", "created_to", "updated_from", "updated_to"
        ]
        filters = {k: clean_text(request.args.get(k)) for k in filter_names}

        view_columns = get_table_columns("vw_chips_painel")
        dim_columns = get_table_columns("dim_chip")
        source_table = "vw_chips_painel" if view_columns else "dim_chip"
        source_columns = view_columns or dim_columns
        if not source_columns:
            raise RuntimeError("Tabela/view de chips não encontrada no dataset configurado.")
        print(f"[Chips] Fonte selecionada: {source_table}. Colunas={sorted(source_columns)}")

        exprs = chip_select_expr(source_columns)
        where, params = ["1=1"], []
        q = filters.get("q")
        if q:
            q_parts = [f"LOWER(COALESCE({exprs['numero']},'')) LIKE @q", f"LOWER(COALESCE({exprs['id_chip']},'')) LIKE @q", f"LOWER(COALESCE({exprs['operador']},'')) LIKE @q"]
            digits = only_digits(q)
            if digits:
                q_parts.append(f"REGEXP_REPLACE(COALESCE({exprs['numero']},''), r'[^0-9]', '') LIKE @q_digits")
                params.append(param("q_digits", "STRING", f"%{digits}%"))
            where.append(f"({' OR '.join(q_parts)})")
            params.append(param("q", "STRING", f"%{q.lower()}%"))

        equals_map = {"status": "status", "operadora": "operadora", "plano": "plano", "operador": "operador", "responsavel": "operador", "tipo_whatsapp": "tipo_whatsapp"}
        for filter_name, alias in equals_map.items():
            value = filters.get(filter_name)
            if value:
                where.append(f"LOWER(COALESCE({exprs[alias]},'')) = @{filter_name}")
                params.append(param(filter_name, "STRING", value.lower()))
        if filters.get("aparelho"):
            where.append(f"CAST({exprs['sk_aparelho_atual']} AS STRING) = @aparelho")
            params.append(param("aparelho", "STRING", filters["aparelho"]))
        if filters.get("ativo") in ["true", "false"]:
            where.append(f"COALESCE({exprs['ativo']}, TRUE) = @ativo")
            params.append(param("ativo", "BOOL", filters["ativo"] == "true"))
        if filters.get("created_from"):
            where.append(f"DATE({exprs['created_at']}) >= @created_from")
            params.append(param("created_from", "DATE", filters["created_from"]))
        if filters.get("created_to"):
            where.append(f"DATE({exprs['created_at']}) <= @created_to")
            params.append(param("created_to", "DATE", filters["created_to"]))
        if filters.get("updated_from"):
            where.append(f"DATE({exprs['updated_at']}) >= @updated_from")
            params.append(param("updated_from", "DATE", filters["updated_from"]))
        if filters.get("updated_to"):
            where.append(f"DATE({exprs['updated_at']}) <= @updated_to")
            params.append(param("updated_to", "DATE", filters["updated_to"]))
        quick = filters.get("quick")
        if quick == "sem_aparelho": where.append(f"{exprs['sk_aparelho_atual']} IS NULL")
        if quick == "banidos": where.append(f"UPPER(COALESCE({exprs['status']},'')) = 'BANIDO'")
        if quick == "recarga": where.append(f"({exprs['ultima_recarga_data']} IS NULL OR {exprs['ultima_recarga_data']} < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))")

        base_sql = f"""
            SELECT {chip_select_list(source_columns)}
            FROM `{PROJECT}.{DATASET}.{source_table}`
            WHERE {' AND '.join(where)}
        """
        sql = f"""
            SELECT *, COUNT(*) OVER() AS total_count
            FROM ({base_sql})
            ORDER BY COALESCE(updated_at, created_at, TIMESTAMP('1970-01-01')) DESC, sk_chip DESC
            LIMIT @limit OFFSET @offset
        """
        query_params = params + [param("limit", "INT64", per_page), param("offset", "INT64", offset)]
        print(f"[Chips] Filtros aplicados: {filters}")
        chips_df = sanitize_df(bq.run_df(sql, params=query_params))
        total = int(chips_df["total_count"].iloc[0]) if not chips_df.empty and "total_count" in chips_df else 0
        chips_records = chips_df.drop(columns=["total_count"], errors="ignore").to_dict("records")
        aparelhos = []
        try:
            aparelhos_df = sanitize_df(bq.run_df(f"SELECT sk_aparelho, marca, modelo FROM `{PROJECT}.{DATASET}.dim_aparelho` ORDER BY marca, modelo LIMIT 500"))
            aparelhos = aparelhos_df.to_dict("records")
        except Exception as exc:
            print(f"[Chips] Aviso: aparelhos não carregados: {exc}")
        stats = {
            "total": total,
            "ativos": sum(1 for c in chips_records if c.get("ativo") is True or str(c.get("status") or "").upper() == "ATIVO"),
            "inativos": sum(1 for c in chips_records if c.get("ativo") is False or str(c.get("status") or "").upper() == "INATIVO"),
            "banidos": sum(1 for c in chips_records if str(c.get("status") or "").upper() == "BANIDO"),
            "disponiveis": sum(1 for c in chips_records if str(c.get("status") or "").upper() in ["DISPONIVEL", "DISPONÍVEL"]),
            "em_uso": sum(1 for c in chips_records if str(c.get("status") or "").upper() in ["EM_USO", "EM USO", "ATIVO"]),
            "com_problema": sum(1 for c in chips_records if str(c.get("status") or "").upper() in ["BANIDO", "BLOQUEADO", "RESTRINGIDO", "MANUTENCAO"]),
            "sem_aparelho": sum(1 for c in chips_records if not c.get("sk_aparelho_atual")),
            "vinculados": sum(1 for c in chips_records if c.get("sk_aparelho_atual")),
            "precisam_recarga": sum(1 for c in chips_records if not c.get("ultima_recarga_data")),
            "total_gasto": sum(float(c.get("total_gasto") or 0) for c in chips_records),
        }
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        print(f"[Chips] Registros retornados={len(chips_records)} total={total} tempo_ms={elapsed_ms}")
        return render_template("chips.html", chips=chips_records, aparelhos=aparelhos, page=page, per_page=per_page, total=total, filters=filters, stats=stats, loading=False)
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        print(f"[Chips] Erro ao carregar chips tempo_ms={elapsed_ms}: {e}")
        return render_template("chips.html", chips=[], aparelhos=[], page=1, per_page=50, total=0, filters={}, stats={}, error="Erro ao carregar chips. Verifique a conexão ou tente novamente.", loading=False), 200

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
        view_columns = get_table_columns("vw_chips_painel")
        dim_columns = get_table_columns("dim_chip")
        source_table = "vw_chips_painel" if view_columns else "dim_chip"
        source_columns = view_columns or dim_columns
        exprs = chip_select_expr(source_columns)
        row = fetch_one(f"""
            SELECT {chip_select_list(source_columns)}
            FROM `{PROJECT}.{DATASET}.{source_table}`
            WHERE {exprs['sk_chip']}=@sk
            LIMIT 1
        """, [param("sk","INT64",sk_chip)])
        return (jsonify(row), 200) if row else (jsonify({"error":"Chip não encontrado"}),404)
    except Exception as e:
        print(f"[Chips] Erro ao buscar chip sk={sk_chip}: {e}"); return jsonify({"error":"Erro interno ao buscar chip"}),500


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
