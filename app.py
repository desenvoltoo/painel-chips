# -*- coding: utf-8 -*-

import os
from flask import Flask, render_template, request, jsonify, redirect
from utils.bigquery_client import BigQueryClient
import pandas as pd

# ===========================
# CONFIGURAÇÃO GERAL
# ===========================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
PORT = int(os.getenv("PORT", 8080))

bq = BigQueryClient()

app = Flask(__name__)


# ===========================
# FUNÇÃO GLOBAL – SANITIZAÇÃO
# ===========================
def sanitize_df(df: pd.DataFrame):
    """
    Padroniza campos para garantir compatibilidade JSON / Jinja.
    """
    for col in df.columns:

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str).replace("NaT", "")

        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].fillna(0)

        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)

        else:
            df[col] = df[col].fillna("")

    return df


# ===========================
# HOME / DASHBOARD
# ===========================
@app.route("/")
@app.route("/dashboard")
def dashboard():

    df = bq.get_view("vw_chips_painel")
    df = sanitize_df(df)
    tabela = df.to_dict(orient="records")

    # KPIs
    total_chips = len(tabela)
    chips_ativos = sum(1 for x in tabela if (x["status"] or "").upper() == "ATIVO")
    disparando = sum(1 for x in tabela if (x["status"] or "").upper() == "DISPARANDO")
    banidos = sum(1 for x in tabela if (x["status"] or "").upper() == "BANIDO")

    # Listas para filtros / gráficos
    lista_status = sorted(list({(x["status"] or "").upper() for x in tabela if x["status"]}))
    lista_operadora = sorted(list({x["operadora"] for x in tabela if x["operadora"]}))

    # ALERTA – chips sem recarga há 80+ dias
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
        ORDER BY dias_sem_recarga DESC;
    """

    alerta = bq._run(alerta_sql).to_dict(orient="records")
    qtd_alerta = len(alerta)

    return render_template(
        "dashboard.html",
        tabela=tabela,
        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,
        lista_status=lista_status,
        lista_operadora=lista_operadora,
        alerta_recarga=alerta,
        qtd_alerta=qtd_alerta
    )


# ===========================
# LISTAGEM DE CHIPS
# ===========================
@app.route("/chips")
def chips():
    df = bq.get_view("vw_chips_painel")
    df = sanitize_df(df)
    tabela = df.to_dict(orient="records")
    return render_template("chips.html", tabela=tabela)


# ===========================
# LISTAGEM DE APARELHOS
# ===========================
@app.route("/aparelhos")
def aparelhos():
    df = bq.get_view("vw_aparelhos")
    df = sanitize_df(df)
    tabela = df.to_dict(orient="records")
    return render_template("aparelhos.html", tabela=tabela)


# ===========================
# MOVIMENTAÇÃO DE CHIP
# ===========================
@app.route("/movimentacao", methods=["GET", "POST"])
def movimentacao():

    if request.method == "GET":
        chips = bq.get_view("vw_chips_painel").to_dict(orient="records")
        aparelhos = bq.get_view("vw_aparelhos").to_dict(orient="records")
        return render_template("movimentacao.html", chips=chips, aparelhos=aparelhos)

    # POST → registrar movimento
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
# RODAR SERVIDOR
# ===========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
