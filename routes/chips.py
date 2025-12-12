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
    """
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
    )


# ============================================================
# CADASTRAR CHIP (UPSERT COMPLETO)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    form = request.form.to_dict()
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
    Busca direto da dim_chip (fonte real do tipo_whatsapp)
    """
    query = f"""
        SELECT
            c.sk_chip,
            c.id_chip,
            c.numero,
            c.operadora,
            c.tipo_whatsapp,
            c.slot_whatsapp,
            c.status,
            c.plano,
            c.operador,
            c.dt_inicio,
            c.ultima_recarga_data,
            c.ultima_recarga_valor,
            c.total_gasto,
            c.observacao
        FROM `{PROJECT}.{DATASET}.dim_chip` c
        WHERE c.sk_chip = {sk_chip}
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

    # força id_chip como string
    if "id_chip" in data:
        data["id_chip"] = str(data["id_chip"])

    bq.upsert_chip(data)
    return jsonify({"success": True})


# ============================================================
# TIMELINE / HISTÓRICO DO CHIP
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    df = bq._run(f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """)

    return jsonify(df.to_dict(orient="records"))
