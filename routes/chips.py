# routes/chips.py
# -*- coding: utf-8 -*-

import os
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")


# ============================================================
# Helpers
# ============================================================
def call_sp(sql: str):
    print("\n🔥 CALL SP ===============================")
    print(sql)
    print("========================================\n")
    return bq.client.query(sql).result()


def sql_str(v):
    if v is None:
        return "NULL"
    v = str(v)
    v = v.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{v}'"


def sql_int(v):
    try:
        if v is None or str(v).strip() == "":
            return "NULL"
        return str(int(float(v)))
    except Exception:
        return "NULL"


def sql_float(v):
    try:
        if v is None or str(v).strip() == "":
            return "NULL"
        return str(float(v))
    except Exception:
        return "NULL"


def sql_date(v):
    """
    Espera "YYYY-MM-DD" ou date-like; retorna DATE('YYYY-MM-DD') ou NULL
    """
    if v is None:
        return "NULL"
    s = str(v).strip()
    if s == "" or s.lower() in ["none", "null", "nan"]:
        return "NULL"
    # se vier "2026-01-05", ok
    return f"DATE('{s}')"


def norm_str(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ["none", "null", "nan"]:
        return None
    return s


# ============================================================
# 📌 LISTAGEM PRINCIPAL
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    try:
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

        return render_template(
            "chips.html",
            chips=chips_df.to_dict(orient="records"),
            aparelhos=aparelhos_df.to_dict(orient="records"),
        )

    except Exception as e:
        print("🚨 Erro ao carregar /chips:", e)
        return "Erro ao carregar chips", 500


# ============================================================
# ➕ CADASTRAR CHIP  (CORRIGIDO: bate com sp_insert_chip)
# sp_insert_chip(
#   p_id_chip STRING,
#   p_numero STRING,
#   p_operadora STRING,
#   p_plano STRING,
#   p_status STRING,
#   p_observacao STRING,
#   p_origem STRING
# )
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        data = request.form.to_dict()

        # normaliza vazios pra NULL
        id_chip = norm_str(data.get("id_chip"))
        numero = norm_str(data.get("numero"))
        operadora = norm_str(data.get("operadora"))
        plano = norm_str(data.get("plano"))
        status = norm_str(data.get("status"))
        observacao = norm_str(data.get("observacao"))  # se seu form tiver esse campo
        origem = "Painel"

        if not numero:
            return """
                <script>
                    alert('Número é obrigatório!');
                    window.location.href='/chips';
                </script>
            """

        call_sp(f"""
            CALL `{PROJECT}.{DATASET}.sp_insert_chip`(
                {sql_str(id_chip)},
                {sql_str(numero)},
                {sql_str(operadora)},
                {sql_str(plano)},
                {sql_str(status)},
                {sql_str(observacao)},
                {sql_str(origem)}
            )
        """)

        return """
            <script>
                alert('Chip cadastrado com sucesso!');
                window.location.href='/chips';
            </script>
        """

    except Exception as e:
        print("🚨 Erro ao cadastrar chip:", e)
        return "Erro ao cadastrar chip", 500


# ============================================================
# 🔍 BUSCAR CHIP (MODAL)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    try:
        df = bq.run_df(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            WHERE sk_chip = {sk_chip}
            LIMIT 1
        """)

        if df.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        return jsonify(sanitize_df(df).iloc[0].to_dict())

    except Exception as e:
        print("🚨 Erro modal:", e)
        return jsonify({"error": "Erro interno"}), 500


# ============================================================
# 💾 ATUALIZAÇÃO COMPLETA (DADOS + STATUS + DATA + APARELHO)
# - ✅ muda dt_inicio mesmo sem mudar status
# - ✅ corrige sp_vincular_aparelho_chip (inclui slot_whatsapp)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        payload = request.json or {}
        sk_chip = payload.get("sk_chip")

        if not sk_chip:
            return jsonify({"error": "sk_chip ausente"}), 400

        # pega atual na DIM (fonte da verdade)
        df_atual = bq.run_df(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip = {int(sk_chip)}
            LIMIT 1
        """)

        if df_atual.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        atual = df_atual.iloc[0].to_dict()

        # ----------------------------------------------------
        # 🔹 1) DADOS BÁSICOS (sp_upsert_chip)
        # sp_upsert_chip(p_sk_chip, p_numero, p_operadora, p_plano, p_observacao, p_operador)
        # ----------------------------------------------------
        numero_novo = norm_str(payload.get("numero"))
        operadora_novo = norm_str(payload.get("operadora"))
        plano_novo = norm_str(payload.get("plano"))
        observacao_novo = norm_str(payload.get("observacao"))
        operador_novo = norm_str(payload.get("operador"))

        if (
            numero_novo != norm_str(atual.get("numero"))
            or operadora_novo != norm_str(atual.get("operadora"))
            or plano_novo != norm_str(atual.get("plano"))
            or observacao_novo != norm_str(atual.get("observacao"))
            or operador_novo != norm_str(atual.get("operador"))
        ):
            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
                    {int(sk_chip)},
                    {sql_str(numero_novo)},
                    {sql_str(operadora_novo)},
                    {sql_str(plano_novo)},
                    {sql_str(observacao_novo)},
                    {sql_str(operador_novo)}
                )
            """)

        # ----------------------------------------------------
        # 🔹 2) STATUS + DATA (sp_alterar_status_chip)
        # sp_alterar_status_chip(p_sk_chip, p_novo_status, p_data_status, p_origem, p_observacao)
        #
        # ✅ regra nova: se mudar a data (dt_inicio) chama a SP mesmo com status igual
        # ----------------------------------------------------
        status_atual = norm_str(atual.get("status"))
        dt_inicio_atual = atual.get("dt_inicio")  # pode ser date/datetime
        dt_inicio_atual_str = str(dt_inicio_atual) if dt_inicio_atual is not None else None

        status_payload_raw = payload.get("status")
        status_payload = norm_str(status_payload_raw)

        # se o usuário não mandou status, mantém o atual
        status_final = status_payload if status_payload is not None else status_atual

        dt_inicio_payload_raw = payload.get("dt_inicio")  # esperado "YYYY-MM-DD"
        dt_inicio_payload_str = norm_str(dt_inicio_payload_raw)

        mudou_status = (status_payload is not None and status_payload != status_atual)
        mudou_data = (dt_inicio_payload_str is not None and dt_inicio_payload_str != dt_inicio_atual_str)

        if mudou_status or mudou_data:
            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_alterar_status_chip`(
                    {int(sk_chip)},
                    {sql_str(status_final)},
                    {sql_date(dt_inicio_payload_str)},
                    'Painel',
                    'Alteração via modal (status/data)'
                )
            """)

        # ----------------------------------------------------
        # 🔹 3) APARELHO (vincular/desvincular)
        # sp_vincular_aparelho_chip(p_sk_chip, p_sk_aparelho, p_slot_whatsapp, p_origem, p_observacao)
        # sp_desvincular_aparelho_chip(p_sk_chip, p_origem, p_observacao)
        # ----------------------------------------------------
        if "sk_aparelho_atual" in payload:
            novo_aparelho = payload.get("sk_aparelho_atual")
            antigo_aparelho = atual.get("sk_aparelho_atual")

            # normaliza (None, "", etc)
            novo_aparelho = None if str(novo_aparelho).strip() in ["", "None", "none", "null", "NULL"] else novo_aparelho
            antigo_aparelho = None if str(antigo_aparelho).strip() in ["", "None", "none", "null", "NULL"] else antigo_aparelho

            if str(novo_aparelho) != str(antigo_aparelho):
                if novo_aparelho is not None:
                    slot = payload.get("slot_whatsapp")  # obrigatório no vínculo
                    if slot in [None, "", "None", "null", "NULL"]:
                        return jsonify({"error": "slot_whatsapp obrigatório ao vincular"}), 400

                    call_sp(f"""
                        CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                            {int(sk_chip)},
                            {int(novo_aparelho)},
                            {int(slot)},
                            'Painel',
                            'Vínculo via painel'
                        )
                    """)
                else:
                    call_sp(f"""
                        CALL `{PROJECT}.{DATASET}.sp_desvincular_aparelho_chip`(
                            {int(sk_chip)},
                            'Painel',
                            'Desvínculo via painel'
                        )
                    """)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro update:", e)
        return jsonify({"error": str(e)}), 500


# ============================================================
# 💰 REGISTRAR RECARGA
# sp_registrar_recarga_chip(p_sk_chip, p_valor, p_origem, p_observacao)
# ============================================================
@chips_bp.route("/chips/recarga", methods=["POST"])
def chips_recarga():
    try:
        payload = request.json or {}

        sk_chip = payload.get("sk_chip")
        valor = payload.get("valor")
        observacao = payload.get("observacao", "Recarga via painel")

        if not sk_chip or valor in [None, ""]:
            return jsonify({"error": "sk_chip e valor obrigatórios"}), 400

        call_sp(f"""
            CALL `{PROJECT}.{DATASET}.sp_registrar_recarga_chip`(
                {int(sk_chip)},
                {sql_float(valor)},
                'Painel',
                {sql_str(observacao)}
            )
        """)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro recarga:", e)
        return jsonify({"error": str(e)}), 500


# ============================================================
# 🧵 TIMELINE
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    try:
        df = bq.run_df(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
            WHERE sk_chip = {int(sk_chip)}
            ORDER BY data_evento DESC
        """)

        return jsonify(sanitize_df(df).to_dict(orient="records"))

    except Exception as e:
        print("🚨 Erro timeline:", e)
        return jsonify([]), 500
