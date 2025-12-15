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
# LISTAGEM PRINCIPAL
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    """
    chips.html espera:
    - chips  -> vw_chips_painel
    - aparelhos -> vw_aparelhos
    """
    try:
        chips_df = sanitize_df(
            bq.get_view("vw_chips_painel")
        )

        aparelhos_df = sanitize_df(
            bq.get_view("vw_aparelhos")
        )

        return render_template(
            "chips.html",
            chips=chips_df.to_dict(orient="records"),
            aparelhos=aparelhos_df.to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro ao carregar chips:", e)
        return "Erro ao carregar chips", 500


# ============================================================
# CADASTRAR CHIP (UPSERT)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        data = request.form.to_dict()

        # garante string
        if "id_chip" in data and data["id_chip"] is not None:
            data["id_chip"] = str(data["id_chip"])

        # normaliza campos vazios
        for k, v in data.items():
            if v == "":
                data[k] = None

        bq.upsert_chip(data)

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
# BUSCAR CHIP PARA MODAL (SEMPRE VIA VIEW)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    try:
        query = f"""
            SELECT
                sk_chip,
                id_chip,
                numero,
                operadora,
                operador,
                status,
                plano,
                data_inicio,
                ultima_recarga_data,
                ultima_recarga_valor,
                total_gasto,
                sk_aparelho,
                aparelho_marca,
                aparelho_modelo
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            WHERE sk_chip = {sk_chip}
            LIMIT 1
        """

        df = bq._run(query)

        if df.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        return jsonify(
            sanitize_df(df).to_dict(orient="records")[0]
        )

    except Exception as e:
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"error": "Erro interno"}), 500


# ============================================================
# SALVAR EDIÇÃO (UPSERT VIA JSON)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        data = request.json or {}

        # garante string
        if "id_chip" in data and data["id_chip"] is not None:
            data["id_chip"] = str(data["id_chip"])

        # normaliza campos vazios
        for k, v in data.items():
            if v == "":
                data[k] = None

        bq.upsert_chip(data)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro ao atualizar chip:", e)
        return jsonify({"error": "Erro ao salvar"}), 500


# ============================================================
# TIMELINE / HISTÓRICO
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    try:
        df = bq._run(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
            WHERE sk_chip = {sk_chip}
            ORDER BY data_evento DESC
        """)

        return jsonify(
            sanitize_df(df).to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro ao carregar timeline:", e)
        return jsonify([]), 500
