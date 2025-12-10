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
# CADASTRAR CHIP (UPsert COMPLETO)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    form = request.form.to_dict()

    # usa o upsert completo (inclui histórico)
    bq.upsert_chip(form)

    return """
        <script>
            alert('Chip cadastrado com sucesso!');
            window.location.href='/chips';
        </script>
    """


# ============================================================
# BUSCAR CHIP PARA EDIÇÃO (carregar dados no modal)
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
# SALVAR EDIÇÃO (USANDO UPSERT COMPLETO)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    data = request.json

    sk = data.get("sk_chip")
    id_chip = data.get("id_chip")

    # Se veio apenas SK, buscamos o ID correspondente
    if sk and not id_chip:
        df = bq._run(f"""
            SELECT id_chip
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip = {sk}
            LIMIT 1
        """)

        if df.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        id_chip = df.iloc[0]["id_chip"]
        data["id_chip"] = id_chip  # adiciona ao JSON antes de chamar o upsert

    # Se MESMO ASSIM não tivermos id_chip, erro real
    if not id_chip:
        return jsonify({"error": "id_chip não enviado"}), 400

    # Agora o upsert funciona perfeitamente
    bq.upsert_chip(data)

    return jsonify({"success": True})

# ============================================================
# TIMELINE / HISTÓRICO DO CHIP
# ============================================================
@chips_bp.route("/chips/timeline/<sk_chip>")
def chips_timeline(sk_chip):
    eventos_df = bq.get_eventos_chip(sk_chip)
    return jsonify(eventos_df.to_dict(orient="records"))
