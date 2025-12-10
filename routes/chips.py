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
# LISTAR CHIPS
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
    aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records"),
    )


# ============================================================
# CADASTRAR CHIP (USANDO UPSERT COMPLETO)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    form = request.form.to_dict()

    # usa a rotina completa — registra histórico e tudo
    bq.upsert_chip(form)

    return """
        <script>
            alert('Chip cadastrado com sucesso!');
            window.location.href='/chips';
        </script>
    """


# ============================================================
# BUSCAR CHIP PARA EDIÇÃO (carrega no modal)
# ============================================================
@chips_bp.route("/chips/sk/<sk_chip>")
def chips_get_by_sk(sk_chip):
    query = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        WHERE sk_chip = {sk_chip}
        LIMIT 1
    """

    df = bq._run(query)

    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    return jsonify(df.to_dict(orient="records")[0])


# ============================================================
# SALVAR EDIÇÃO (UPERT COMPLETO + HISTÓRICO)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    data = request.json

    # usa a rotina oficial que:
    # ✔ compara valores antigos
    # ✔ registra eventos na auditoria
    # ✔ executa SP de registro
    # ✔ salva o chip já normalizado
    bq.upsert_chip(data)

    return jsonify({"success": True})


# ============================================================
# TIMELINE / HISTÓRICO DO CHIP
# ============================================================
@chips_bp.route("/chips/timeline/<sk_chip>")
def chips_timeline(sk_chip):

    eventos_df = bq.get_eventos_chip(sk_chip)

    return jsonify(eventos_df.to_dict(orient="records"))
