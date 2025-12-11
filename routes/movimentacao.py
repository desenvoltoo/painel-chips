# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery

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
# 📜 API — HISTÓRICO DO CHIP (Eventos DO PAINEL)
# ============================================================
@mov_bp.route("/movimentacao/historico/<sk_chip>")
def historico_chip(sk_chip):

    try:
        # Garante que sk_chip seja inteiro
        sk_chip_int = int(sk_chip)

        # SQL seguro com parâmetros
        sql = f"""
            SELECT
              sk_chip,
              categoria,
              tipo_evento,
              campo,
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
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip_int),
                bigquery.ScalarQueryParameter("origem", "STRING", "Painel")
            ]
        )

        query_job = bq.client.query(sql, job_config=job_config)
        rows = query_job.result()

        # Converte para JSON manualmente, sem pandas
        events = []
        for row in rows:
            events.append({
                "sk_chip": row.sk_chip,
                "categoria": row.categoria,
                "tipo_evento": row.tipo_evento,
                "campo": row.campo,
                "valor_antigo": row.valor_antigo,
                "valor_novo": row.valor_novo,
                "origem": row.origem,
                "observacao": row.observacao,
                "data_evento": str(row.data_evento),
                "data_fmt": row.data_fmt
            })

        return jsonify(events)

    except Exception as e:
        print("🚨 Erro ao buscar histórico:", e)
        return jsonify([]), 500
