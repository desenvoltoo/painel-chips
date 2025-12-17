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
# 🔧 UTIL — EXECUTAR STORED PROCEDURE
# ============================================================
def call_sp(sql: str):
    bq.client.query(sql).result()


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
            aparelhos=aparelhos_df.to_dict(orient="records")
        )
    except Exception as e:
        print("🚨 Erro ao carregar chips:", e)
        return "Erro ao carregar chips", 500


# ============================================================
# ➕ CADASTRAR CHIP
# SP: sp_upsert_chip(p_id_chip, p_numero, p_operadora, p_plano, p_status)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        data = request.form.to_dict()

        for k in list(data.keys()):
            if data[k] == "":
                data[k] = None

        sql = f"""
        CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
            {data.get("id_chip") and f"'{data['id_chip']}'" or "NULL"},
            {data.get("numero") and f"'{data['numero']}'" or "NULL"},
            {data.get("operadora") and f"'{data['operadora']}'" or "NULL"},
            {data.get("plano") and f"'{data['plano']}'" or "NULL"},
            {data.get("status") and f"'{data['status']}'" or "NULL"}
        )
        """
        call_sp(sql)

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
# 🔍 BUSCAR CHIP (MODAL EDIÇÃO)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    try:
        df = bq._run(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            WHERE sk_chip = {sk_chip}
            LIMIT 1
        """)

        if df.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        return jsonify(sanitize_df(df).iloc[0].to_dict())
    except Exception as e:
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"error": "Erro interno"}), 500


# ============================================================
# 💾 SALVAR EDIÇÃO
# - sp_upsert_chip  → dados básicos
# - sp_alterar_status_chip → se status mudou
# - sp_vincular_aparelho_chip → se aparelho veio
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        payload = request.json or {}

        if not payload.get("sk_chip") or not payload.get("id_chip"):
            return jsonify({"error": "sk_chip ou id_chip ausente"}), 400

        sk_chip = payload["sk_chip"]

        for k in list(payload.keys()):
            if payload[k] == "":
                payload[k] = None

        # ====================================================
        # 🔹 ATUALIZA DADOS BÁSICOS
        # ====================================================
        sql = f"""
        CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
            '{payload["id_chip"]}',
            {payload.get("numero") and f"'{payload['numero']}'" or "NULL"},
            {payload.get("operadora") and f"'{payload['operadora']}'" or "NULL"},
            {payload.get("plano") and f"'{payload['plano']}'" or "NULL"},
            {payload.get("status") and f"'{payload['status']}'" or "NULL"}
        )
        """
        call_sp(sql)

        # ====================================================
        # 🔹 STATUS (se enviado explicitamente)
        # ====================================================
        if payload.get("status"):
            sql = f"""
            CALL `{PROJECT}.{DATASET}.sp_alterar_status_chip`(
                {sk_chip},
                '{payload["status"]}',
                CURRENT_DATE(),
                'Painel',
                'Alteração via painel'
            )
            """
            call_sp(sql)

        # ====================================================
        # 🔹 VINCULAR APARELHO (se veio)
        # ====================================================
        if payload.get("sk_aparelho_atual") is not None:
            sql = f"""
            CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                {sk_chip},
                {payload["sk_aparelho_atual"]},
                'Painel',
                'Vinculação via painel'
            )
            """
            call_sp(sql)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro ao atualizar chip:", e)
        return jsonify({"error": "Erro ao salvar"}), 500


# ============================================================
# 🧵 TIMELINE / HISTÓRICO (LEITURA)
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

        return jsonify(sanitize_df(df).to_dict(orient="records"))
    except Exception as e:
        print("🚨 Erro ao carregar timeline:", e)
        return jsonify([]), 500
