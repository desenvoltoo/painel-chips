# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

mov_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()


# ============================================================
# 📌 PÁGINA PRINCIPAL — Movimentação + Timeline
# ============================================================
@mov_bp.route("/movimentacao")
def movimentacao_home():

    try:
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

        return render_template(
            "movimentacao.html",
            chips=chips_df.to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro ao carregar página de movimentação:", e)
        return "Erro ao carregar movimentação", 500


# ============================================================
# 🔍 AUTOCOMPLETE — Buscar chip digitando
# ============================================================
@mov_bp.route("/movimentacao/buscar")
def buscar_chip():

    try:
        termo = request.args.get("q", "").strip().lower()

        if len(termo) < 2:
            return jsonify([])

        df = sanitize_df(bq.get_view("vw_chips_painel"))

        # Filtra número ou operadora
        filtrado = df[
            df["numero"].astype(str).str.contains(termo, case=False, na=False)
        ]

        return jsonify(
            filtrado[["sk_chip", "numero", "operadora"]].to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro no autocomplete:", e)
        return jsonify([])


# ============================================================
# 📜 API — HISTÓRICO COMPLETO DO CHIP (Eventos + Movimentos)
# ============================================================
@mov_bp.route("/movimentacao/historico/<sk_chip>")
def historico_chip(sk_chip):

    try:
        sql = f"""
            SELECT *
            FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
            WHERE sk_chip = {sk_chip}
            ORDER BY data_evento DESC
        """

        df = bq._run(sql)
        df = sanitize_df(df)

        return jsonify(df.to_dict(orient="records"))

    except Exception as e:
        print("🚨 Erro ao buscar histórico:", e)
        return jsonify([]), 500
