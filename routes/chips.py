# routes/chips.py
# -*- coding: utf-8 -*-

import os
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")


# ============================================================
# 🔧 EXECUTAR STORED PROCEDURE (LOG + CONTROLE)
# ============================================================
def call_sp(sql: str):
    print("\n🔥 CALL SP ===============================")
    print(sql)
    print("========================================\n")
    return bq.client.query(sql).result()


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
            aparelhos=aparelhos_df.to_dict(orient="records"),
        )

    except Exception as e:
        print("🚨 Erro ao carregar /chips:", e)
        return "Erro ao carregar chips", 500


# ============================================================
# ➕ CADASTRAR CHIP (INSERT REAL)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        data = request.form.to_dict()

        for k in data:
            if data[k] == "":
                data[k] = None

        call_sp(f"""
            CALL `{PROJECT}.{DATASET}.sp_insert_chip`(
                {f"'{data['id_chip']}'" if data.get("id_chip") else "NULL"},
                '{data["numero"]}',
                {f"'{data['operadora']}'" if data.get("operadora") else "NULL"},
                {f"'{data['plano']}'" if data.get("plano") else "NULL"},
                {f"'{data['status']}'" if data.get("status") else "NULL"},
                'Painel'
            )
        """)

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
# 🔍 BUSCAR CHIP (MODAL)
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
# 💾 SALVAR EDIÇÃO (FLUXO CORRETO + HISTÓRICO)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
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

        for k in payload:
            if payload[k] == "":
                payload[k] = None

        # ----------------------------------------------------
        # 🔹 ATUALIZA DADOS BÁSICOS
        # ----------------------------------------------------
        if (
            payload.get("numero") != atual.get("numero")
            or payload.get("operadora") != atual.get("operadora")
            or payload.get("plano") != atual.get("plano")
            or payload.get("observacao") != atual.get("observacao")
        ):
            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
                    {sk_chip},
                    {f"'{payload['numero']}'" if payload.get("numero") else "NULL"},
                    {f"'{payload['operadora']}'" if payload.get("operadora") else "NULL"},
                    {f"'{payload['plano']}'" if payload.get("plano") else "NULL"},
                    {f"'{payload['observacao']}'" if payload.get("observacao") else "NULL"}
                )
            """)

        # ----------------------------------------------------
        # 🔹 ALTERAÇÃO DE STATUS (COM DATA DO STATUS)
        # ----------------------------------------------------
        if payload.get("status") and payload["status"] != atual.get("status"):
            call_sp(f"""
                CALL `{PROJECT}.{DATASET}.sp_alterar_status_chip`(
                    {sk_chip},
                    '{payload["status"]}',
                    CURRENT_DATE(),          -- ✅ data do status
                    'Painel',
                    'Alteração via painel'
                )
            """)

        # ----------------------------------------------------
        # 🔹 VÍNCULO / DESVÍNCULO DE APARELHO
        # ----------------------------------------------------
        if "sk_aparelho_atual" in payload:
            novo = payload.get("sk_aparelho_atual")
            antigo = atual.get("sk_aparelho_atual")

            if novo != antigo:
                if novo:
                    call_sp(f"""
                        CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                            {sk_chip},
                            {novo},
                            'Painel',
                            'Vínculo via painel'
                        )
                    """)
                else:
                    call_sp(f"""
                        CALL `{PROJECT}.{DATASET}.sp_desvincular_aparelho_chip`(
                            {sk_chip},
                            'Painel',
                            'Desvínculo via painel'
                        )
                    """)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro ao atualizar chip:", e)
        return jsonify({"error": str(e)}), 500


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
