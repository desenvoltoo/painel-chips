# -*- coding: utf-8 -*-

import os
import pandas as pd
from flask import Flask, render_template, request, jsonify

# Utils
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

# Blueprints (agora corretos)
from routes.aparelhos import bp_aparelhos
from routes.chips import chips_bp
from routes.recargas import recargas_bp
from routes.relacionamentos import relacionamentos_bp
from routes import register_blueprints


# ===========================
# CONFIGURAÇÃO GERAL
# ===========================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
PORT = int(os.getenv("PORT", 8080))

app = Flask(__name__)
bq = BigQueryClient()


# ===========================
# DASHBOARD COMPLETO
# ===========================
@app.route("/")
@app.route("/dashboard")
def dashboard():

    # ------ 1) TABELA PRINCIPAL ------
    df = bq.get_view("vw_chips_painel")
    df = sanitize_df(df)

    tabela = df.to_dict(orient="records")

    # ------ 2) KPIs ------
    total_chips = len(tabela)
    chips_ativos = sum(1 for x in tabela if (x["status"] or "").upper() == "ATIVO")
    disparando = sum(1 for x in tabela if (x["status"] or "").upper() == "DISPARANDO")
    banidos = sum(1 for x in tabela if (x["status"] or "").upper() == "BANIDO")

    # ------ 3) LISTAS PARA GRÁFICOS E FILTROS ------
    lista_status = sorted(list({ (x["status"] or "").upper() for x in tabela if x["status"] }))
    lista_operadora = sorted(list({ x["operadora"] for x in tabela if x["operadora"] }))

    # ------ 4) ALERTA DE RECARGA (chips > 80 dias sem recarga) ------
    alerta_sql = f"""
        SELECT
            numero,
            status,
            operadora,
            ultima_recarga_data,
            DATE_DIFF(CURRENT_DATE(), DATE(ultima_recarga_data), DAY) AS dias_sem_recarga
        FROM `{PROJECT_ID}.{DATASET}.vw_chips_painel`
        WHERE ultima_recarga_data IS NOT NULL
        AND DATE_DIFF(CURRENT_DATE(), DATE(ultima_recarga_data), DAY) > 80
        ORDER BY dias_sem_recarga DESC
    """
    alerta = bq._run(alerta_sql).to_dict(orient="records")

    return render_template(
        "dashboard.html",
        tabela=tabela,

        # KPIs
        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,

        # filtros e gráficos
        lista_status=lista_status,
        lista_operadora=lista_operadora,

        # alertas
        alerta_recarga=alerta,
        qtd_alerta=len(alerta)
    )

# ===========================
# MOVIMENTAÇÃO
# ===========================
@app.route("/movimentacao", methods=["GET", "POST"])
def movimentacao():

    if request.method == "GET":

        chips = bq.get_view("vw_chips_painel")
        chips = sanitize_df(chips).to_dict(orient="records")

        aparelhos = bq.get_view("vw_aparelhos")
        aparelhos = sanitize_df(aparelhos).to_dict(orient="records")

        return render_template(
            "movimentacao.html",
            chips=chips,
            aparelhos=aparelhos
        )

    # POST
    data = request.form.to_dict()

    sk_chip = int(data.get("sk_chip"))
    sk_aparelho = int(data.get("sk_aparelho")) if data.get("sk_aparelho") else None
    tipo = data.get("tipo_movimento")
    origem = data.get("origem", "Painel")
    observacao = data.get("observacao", "")

    ok = bq.registrar_movimento_chip(
        sk_chip=sk_chip,
        sk_aparelho=sk_aparelho,
        tipo=tipo,
        origem=origem,
        observacao=observacao
    )

    return jsonify({"status": "ok" if ok else "erro"})


# ===========================
# BLUEPRINTS
# ===========================
app.register_blueprint(bp_aparelhos)
app.register_blueprint(chips_bp)
app.register_blueprint(recargas_bp)
app.register_blueprint(relacionamentos_bp)
app.register_blueprints(app)

# ===========================
# RUN
# ===========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
