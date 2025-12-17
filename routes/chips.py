# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
import os

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")


# ============================================================
# 🔧 UTIL
# ============================================================
def call_sp(sql: str):
    bq.run(sql)


# ============================================================
# 📌 LISTAGEM PRINCIPAL
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
    aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )


# ============================================================
# ➕ CADASTRAR CHIP
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    data = request.form.to_dict()

    for k in data:
        if data[k] == "":
            data[k] = None

    call_sp(f"""
        CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
            {f"'{data['id_chip']}'" if data.get("id_chip") else "NULL"},
            {f"'{data['numero']}'" if data.get("numero") else "NULL"},
            {f"'{data['operadora']}'" if data.get("operadora") else "NULL"},
            {f"'{data['plano']}'" if data.get("plano") else "NULL"},
            {f"'{data['status']}'" if data.get("status") else "NULL"}
        )
    """)

    return """
        <script>
            alert('Chip cadastrado com sucesso!');
            window.location.href='/chips';
        </script>
    """


# ============================================================
# 🔍 BUSCAR CHIP (MODAL)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    df = bq.run_df(f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        WHERE sk_chip = {sk_chip}
        LIMIT 1
    """)

    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    return jsonify(sanitize_df(df).iloc[0].to_dict())


# ============================================================
# 💾 SALVAR EDIÇÃO (SEM DUPLICAR EVENTOS)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():

    payload = request.json or {}
    sk_chip = payload.get("sk_chip")

    if not sk_chip:
        return jsonify({"error": "sk_chip ausente"}), 400

    df_atual = bq.run_df(f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.dim_chip`
        WHERE sk_chip = {sk_chip}
    """)

    if df_atual.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    atual = df_atual.iloc[0].to_dict()
    id_chip = atual["id_chip"]

    for k in payload:
        if payload[k] == "":
            payload[k] = None

    # ========================================================
    # 🔹 DADOS BÁSICOS
    # ========================================================
    if (
        payload.get("numero") != atual.get("numero") or
        payload.get("operadora") != atual.get("operadora") or
        payload.get("plano") != atual.get("plano")
    ):
        call_sp(f"""
            CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
                '{id_chip}',
                {f"'{payload['numero']}'" if payload.get("numero") else "NULL"},
                {f"'{payload['operadora']}'" if payload.get("operadora") else "NULL"},
                {f"'{payload['plano']}'" if payload.get("plano") else "NULL"},
                '{atual.get("status")}'
            )
        """)

        call_sp(f"""
            CALL `{PROJECT}.{DATASET}.sp_registrar_evento_chip`(
                {sk_chip},
                'DADOS',
                'ALTERACAO_DADOS',
                'dados_basicos',
                'ANTES',
                'DEPOIS',
                'Painel',
                'Alteração de dados do chip'
            )
        """)

    # ========================================================
    # 🔹 STATUS (SP JÁ REGISTRA EVENTO)
    # ========================================================
    if payload.get("status") and payload["status"] != atual.get("status"):
        call_sp(f"""
            CALL `{PROJECT}.{DATASET}.sp_alterar_status_chip`(
                {sk_chip},
                '{payload["status"]}',
                'Painel',
                'Alteração via painel'
            )
        """)

    # ========================================================
    # 🔹 APARELHO (SP JÁ REGISTRA EVENTO)
    # ========================================================
    if "sk_aparelho_atual" in payload:
        if payload["sk_aparelho_atual"] != atual.get("sk_aparelho_atual"):
            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                    {sk_chip},
                    {payload["sk_aparelho_atual"] if payload["sk_aparelho_atual"] is not None else "NULL"},
                    'Painel',
                    'Troca de aparelho'
                )
            """)

    return jsonify({"success": True})


# ============================================================
# 🧵 TIMELINE
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    df = bq.run_df(f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """)

    return jsonify(sanitize_df(df).to_dict(orient="records"))
