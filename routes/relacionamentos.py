# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# ============================================================
# 📌 PÁGINA PRINCIPAL — APARELHOS + CHIPS + SLOTS
# ============================================================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():

    try:
        # -------------------------------
        # 1. CARREGAR TODOS OS APARELHOS
        # -------------------------------
        aparelhos_df = sanitize_df(bq.query("""
            SELECT 
                sk_aparelho,
                marca,
                modelo,
                capacidade_whatsapp
            FROM `painel-universidade.marts.dim_aparelho`
            ORDER BY marca, modelo
        """))

        # -------------------------------
        # 2. CARREGAR TODOS OS CHIPS
        # -------------------------------
        chips_df = sanitize_df(bq.query("""
            SELECT 
                sk_chip,
                numero,
                operadora,
                status,
                sk_aparelho_atual,
                slot_whatsapp
            FROM `painel-universidade.marts.dim_chip`
            ORDER BY numero
        """))

        aparelhos = aparelhos_df.to_dict(orient="records")
        chips = chips_df.to_dict(orient="records")

        return render_template(
            "relacionamentos.html",
            aparelhos=aparelhos,
            chips=chips
        )

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# 🔄 VINCULAR — CHIP → APARELHO + SLOT
# ============================================================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():

    try:
        dados = request.get_json(silent=True) or {}

        sk_chip = dados.get("sk_chip")
        sk_aparelho = dados.get("sk_aparelho")
        slot = dados.get("slot")

        if not sk_chip or not sk_aparelho or slot is None:
            return jsonify({"erro": "Dados incompletos"}), 400

        sql = f"""
        UPDATE `painel-universidade.marts.dim_chip`
        SET 
            sk_aparelho_atual = {sk_aparelho},
            slot_whatsapp = {slot},
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {sk_chip}
        """

        bq.execute_query(sql)
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO AO VINCULAR SLOT:", e)
        return jsonify({"erro": "Falha ao vincular"}), 500


# ============================================================
# ❌ DESVINCULAR — CHIP → LIBERAR APARELHO + SLOT
# ============================================================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():

    try:
        dados = request.get_json(silent=True) or {}
        sk_chip = dados.get("sk_chip")

        if not sk_chip:
            return jsonify({"erro": "sk_chip ausente"}), 400

        sql = f"""
        UPDATE `painel-universidade.marts.dim_chip`
        SET 
            sk_aparelho_atual = NULL,
            slot_whatsapp = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {sk_chip}
        """

        bq.execute_query(sql)
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO AO DESVINCULAR SLOT:", e)
        return jsonify({"erro": "Falha ao desvincular"}), 500
