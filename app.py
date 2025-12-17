# -*- coding: utf-8 -*-

import os
from flask import Flask, render_template

# Utils
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

# Blueprints
from routes.aparelhos import aparelhos_bp
from routes.chips import chips_bp
from routes.recargas import recargas_bp
from routes.relacionamentos import relacionamentos_bp
from routes.movimentacao import mov_bp

# ================================
# CONFIGURAÇÃO GERAL
# ================================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
PORT = int(os.getenv("PORT", 8080))

app = Flask(__name__)
bq = BigQueryClient()

# ================================
# DASHBOARD PRINCIPAL
# ================================
@app.route("/")
@app.route("/dashboard")
def dashboard():

    # 1 — Dados principais
    df = bq.get_view("vw_chips_painel")
    df = sanitize_df(df)
    tabela = df.to_dict(orient="records")

    # 2 — KPIs
    total_chips = len(tabela)
    chips_ativos = sum(1 for x in tabela if (x.get("status") or "").upper() == "ATIVO")
    disparando = sum(1 for x in tabela if (x.get("status") or "").upper() == "DISPARANDO")
    banidos = sum(1 for x in tabela if (x.get("status") or "").upper() == "BANIDO")

    # 3 — Filtros
    lista_status = sorted({(x.get("status") or "").upper() for x in tabela if x.get("status")})
    lista_operadora = sorted({x["operadora"] for x in tabela if x.get("operadora")})

    # 4 — ALERTAS DE RECARGA (SEM _run ❌)
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

    alerta_df = bq.run_df(alerta_sql)
    alerta_df = sanitize_df(alerta_df)
    alerta_recarga = alerta_df.to_dict(orient="records")

    return render_template(
        "dashboard.html",
        tabela=tabela,

        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,

        lista_status=lista_status,
        lista_operadora=lista_operadora,

        alerta_recarga=alerta_recarga,
        qtd_alerta=len(alerta_recarga),
    )

# ================================
# REGISTRO DOS BLUEPRINTS
# ================================
app.register_blueprint(aparelhos_bp)
app.register_blueprint(chips_bp)
app.register_blueprint(recargas_bp)
app.register_blueprint(relacionamentos_bp)
app.register_blueprint(mov_bp)

# ================================
# RUN SERVER
# ================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
