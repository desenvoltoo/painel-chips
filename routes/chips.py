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
    bq.run(sql)


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
            {f"'{data['id_chip']}'" if data.get("id_chip") else "NULL"},
            {f"'{data['numero']}'" if data.get("numero") else "NULL"},
            {f"'{data['operadora']}'" if data.get("operadora") else "NULL"},
            {f"'{data['plano']}'" if data.get("plano") else "NULL"},
            {f"'{data['status']}'" if data.get("status") else "NULL"}
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
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"error": "Erro interno"}), 500


# ============================================================
# 💾 SALVAR EDIÇÃO + EVENTOS
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        payload = request.json or {}

        if not payload.get("sk_chip"):
            return jsonify({"error": "sk_chip ausente"}), 400

        sk_chip = payload["sk_chip"]

        # ----------------------------------------------------
        # 🔎 ESTADO ATUAL
        # ----------------------------------------------------
        df_atual = bq.run_df(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip = {sk_chip}
        """)

        if df_atual.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        atual = df_atual.iloc[0].to_dict()
        id_chip = atual["id_chip"]

        for k in list(payload.keys()):
            if payload[k] == "":
                payload[k] = None

        # ----------------------------------------------------
        # 🔹 ALTERAÇÃO DE DADOS (NUMERO / OPERADORA / PLANO)
        # ----------------------------------------------------
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
                    'ALTERACAO_DADOS',
                    'DADOS_ANTERIORES',
                    'DADOS_ATUALIZADOS',
                    'Painel',
                    'Alteração de dados do chip'
                )
            """)

        # ----------------------------------------------------
        # 🔹 ALTERAÇÃO DE STATUS
        # ----------------------------------------------------
        if payload.get("status") and payload["status"] != atual.get("status"):
            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_alterar_status_chip`(
                    {sk_chip},
                    '{payload["status"]}',
                    CURRENT_DATE(),
                    'Painel',
                    'Alteração via painel'
                )
            """)

            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_registrar_evento_chip`(
                    {sk_chip},
                    'ALTERACAO_STATUS',
                    '{atual.get("status")}',
                    '{payload["status"]}',
                    'Painel',
                    'Status alterado via painel'
                )
            """)

        # ----------------------------------------------------
        # 🔹 ALTERAÇÃO DE APARELHO
        # ----------------------------------------------------
        if "sk_aparelho_atual" in payload:
            if payload["sk_aparelho_atual"] != atual.get("sk_aparelho_atual"):

                call_sp(f"""
                    CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                        {sk_chip},
                        {payload["sk_aparelho_atual"] if payload["sk_aparelho_atual"] is not None else "NULL"},
                        'Painel',
                        'Vinculação via painel'
                    )
                """)

                call_sp(f"""
                    CALL `{PROJECT}.{DATASET}.sp_registrar_evento_chip`(
                        {sk_chip},
                        'ALTERACAO_APARELHO',
                        {f"'{atual.get('sk_aparelho_atual')}'" if atual.get("sk_aparelho_atual") else "NULL"},
                        {f"'{payload['sk_aparelho_atual']}'" if payload.get("sk_aparelho_atual") else "NULL"},
                        'Painel',
                        'Troca de aparelho'
                    )
                """)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro ao atualizar chip:", e)
        return jsonify({"error": "Erro ao salvar"}), 500


# ============================================================
# 🧵 TIMELINE / HISTÓRICO
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    try:
        df = bq.run_df(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
            WHERE sk_chip = {sk_chip}
            ORDER BY data_evento DESC
        """)

        return jsonify(sanitize_df(df).to_dict(orient="records"))

    except Exception as e:
        print("🚨 Erro ao carregar timeline:", e)
        return jsonify([]), 500
