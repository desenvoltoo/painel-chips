# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
import datetime

rel_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()

# ============================================================
# 📌 PÁGINA PRINCIPAL
# ============================================================
@rel_bp.route("/relacionamentos")
def relacionamentos_home():

    try:
        df = sanitize_df(bq.get_view("vw_relacionamentos_whatsapp"))

        return render_template(
            "relacionamentos.html",
            aparelhos=df.to_dict(orient="records")
        )

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# 🔄 ATUALIZAR UM SLOT: vincular chip → slot
# ============================================================
@rel_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():

    try:
        sk_chip = request.json.get("sk_chip")
        slot = request.json.get("slot")

        if not sk_chip or slot is None:
            return jsonify({"erro": "Dados incompletos"}), 400

        sql = f"""
        UPDATE `painel-universidade.marts.dim_chip`
        SET slot_whatsapp = {slot},
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {sk_chip}
        """

        bq.execute_query(sql)

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO AO VINCULAR SLOT:", e)
        return jsonify({"erro": "Falha ao vincular"}), 500


# ============================================================
# ❌ DESVINCULAR SLOT: remover chip → slot
# ============================================================
@rel_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():

    try:
        sk_chip = request.json.get("sk_chip")

        if not sk_chip:
            return jsonify({"erro": "sk_chip ausente"}), 400

        sql = f"""
        UPDATE `painel-universidade.marts.dim_chip`
        SET slot_whatsapp = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {sk_chip}
        """

        bq.execute_query(sql)

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO AO DESVINCULAR:", e)
        return jsonify({"erro": "Falha ao desvincular"}), 500
