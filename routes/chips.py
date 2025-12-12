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
# LISTAR CHIPS (PAINEL PRINCIPAL)
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    """
    Usa vw_chips_painel
    Campos existentes:
    sk_chip, numero, operadora, tipo_whatsapp,
    sk_aparelho, slot_whatsapp,
    aparelho_marca, aparelho_modelo
    """
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
    aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records"),
    )


# ============================================================
# CADASTRAR CHIP (UPSERT COMPLETO)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    form = request.form.to_dict()

    # upsert já grava em dim_chip + histórico
    bq.upsert_chip(form)

    return """
        <script>
            alert('Chip cadastrado com sucesso!');
            window.location.href='/chips';
        </script>
    """


# ============================================================
# BUSCAR CHIP PARA EDIÇÃO (MODAL)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    """
    vw_chips_painel NÃO TEM id_chip
    então buscamos estado visual + id_chip da dim_chip
    """
    query = f"""
        SELECT
            p.*,
            d.id_chip
        FROM `{PROJECT}.{DATASET}.vw_chips_painel` p
        LEFT JOIN `{PROJECT}.{DATASET}.dim_chip` d
            ON p.sk_chip = d.sk_chip
        WHERE p.sk_chip = {sk_chip}
        LIMIT 1
    """

    df = bq._run(query)

    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    return jsonify(df.to_dict(orient="records")[0])


# ============================================================
# SALVAR EDIÇÃO (UPSERT COM HISTÓRICO)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    data = request.json or {}

    sk_chip = data.get("sk_chip")
    id_chip = data.get("id_chip")

    # Garante id_chip (obrigatório no upsert)
    if sk_chip and not id_chip:
        df = bq._run(f"""
            SELECT id_chip
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip = {sk_chip}
            LIMIT 1
        """)

        if df.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        id_chip = str(df.iloc[0]["id_chip"])
        data["id_chip"] = id_chip

    # força string
    data["id_chip"] = str(data["id_chip"])

    # UPSERT COMPLETO
    bq.upsert_chip(data)

    return jsonify({"success": True})


# ============================================================
# TIMELINE / HISTÓRICO DO CHIP
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    """
    Usa vw_chip_timeline
    Campos:
    categoria, tipo_evento, campo,
    valor_antigo, valor_novo,
    origem, observacao, data_evento, data_fmt
    """
    df = bq._run(f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """)

    return jsonify(df.to_dict(orient="records"))
