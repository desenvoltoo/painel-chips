# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery

mov_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()


# ============================================================
# 📌 PÁGINA PRINCIPAL — MOVIMENTAÇÃO
# ============================================================
@mov_bp.route("/movimentacao")
def movimentacao_home():
    try:
        chips_df = sanitize_df(
            bq.get_view("vw_chips_painel")
        )

        return render_template(
            "movimentacao.html",
            chips=chips_df.to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro ao carregar página de movimentação:", e)
        return "Erro ao carregar movimentação", 500


# ============================================================
# 🔍 AUTOCOMPLETE — BUSCAR CHIP
# ============================================================
@mov_bp.route("/movimentacao/buscar")
def buscar_chip():
    try:
        termo = request.args.get("q", "").strip().lower()

        if len(termo) < 2:
            return jsonify([])

        df = sanitize_df(
            bq.get_view("vw_chips_painel")
        )

        filtrado = df[
            df["numero"].astype(str).str.contains(termo, case=False, na=False)
        ]

        return jsonify(
            filtrado[["sk_chip", "numero", "operadora"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro no autocomplete:", e)
        return jsonify([]), 500


# ============================================================
# 📜 HISTÓRICO / TIMELINE DO CHIP
# ============================================================
@mov_bp.route("/movimentacao/historico/<int:sk_chip>")
def historico_chip(sk_chip):
    try:
        sql = f"""
            SELECT
                sk_chip,
                categoria,
                tipo,
                valor_antigo,
                valor_novo,
                origem,
                observacao,
                data_evento,
                data_fmt
            FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
            WHERE sk_chip = @sk_chip
              AND origem = @origem
            ORDER BY data_evento DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "sk_chip", "INT64", sk_chip
                ),
                bigquery.ScalarQueryParameter(
                    "origem", "STRING", "Painel"
                )
            ]
        )

        rows = bq.client.query(sql, job_config=job_config).result()

        eventos = []
        for r in rows:
            eventos.append({
                "sk_chip": r.sk_chip,
                "categoria": r.categoria,
                "tipo": r.tipo,
                "valor_antigo": r.valor_antigo,
                "valor_novo": r.valor_novo,
                "origem": r.origem,
                "observacao": r.observacao,
                "data_evento": (
                    r.data_evento.isoformat()
                    if r.data_evento else None
                ),
                "data_fmt": r.data_fmt
            })

        return jsonify(eventos)

    except Exception as e:
        print("🚨 Erro ao buscar histórico:", e)
        return jsonify([]), 500
