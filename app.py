# app.py
from flask import Flask, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from datetime import datetime
import pandas as pd

app = Flask(__name__)
bq = BigQueryClient()


# =======================================================
# DASHBOARD
# =======================================================
@app.route("/")
@app.route("/dashboard")
def dashboard():
    df = bq.get_view()

    # === KPIs ===
    total_chips = len(df)
    chips_ativos = len(df[df["ativo"] == True])
    disparando = len(df[df["status"] == "DISPARANDO"])
    banidos = len(df[df["status"] == "BANIDO"])

    # === ALERTA RECARGA ===
    hoje = datetime.now().date()
    df["dias_sem_recarga"] = df["ultima_recarga_data"].apply(
        lambda x: (hoje - x).days if pd.notnull(x) else 999
    )
    alerta_recarga = df[df["dias_sem_recarga"] >= 80]

    # === FILTROS ===
    lista_status = sorted(df["status"].dropna().unique())
    lista_operadora = sorted(df["operadora"].dropna().unique())
    lista_aparelho = sorted(df["nome_aparelho"].dropna().unique())

    return render_template(
        "dashboard.html",
        tabela=df.to_dict(orient="records"),
        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,
        lista_status=lista_status,
        lista_operadora=lista_operadora,
        lista_aparelho=lista_aparelho,
        alerta_recarga=alerta_recarga.to_dict(orient="records"),
        qtd_alerta=len(alerta_recarga),
    )


# =======================================================
# APARELHOS — LISTA
# =======================================================
@app.route("/aparelhos")
def aparelhos():
    aparelhos_df = bq.get_aparelhos()

    return render_template(
        "aparelhos.html",
        aparelhos=aparelhos_df.to_dict(orient="records"),
    )


# =======================================================
# APARELHOS — UPSERT
# =======================================================
@app.route("/aparelhos/add", methods=["POST"])
def add_aparelho():
    bq.upsert_aparelho(request.form)
    return redirect("/aparelhos")


# =======================================================
# CHIPS — LISTA
# =======================================================
@app.route("/chips")
def chips():
    chips_df = bq.get_chips()
    aparelhos_df = bq.get_aparelhos()

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records"),
    )


# =======================================================
# CHIPS — UPSERT
# =======================================================
@app.route("/chips/add", methods=["POST"])
def add_chip():
    bq.upsert_chip(request.form)
    return redirect("/chips")


# =======================================================
# MOVIMENTAÇÃO — LISTA
# =======================================================
@app.route("/movimentacao")
def movimentacao():
    eventos_df = bq.get_eventos()

    return render_template(
        "movimentacao.html",
        eventos=eventos_df.to_dict(orient="records"),
    )


# =======================================================
# MOVIMENTAÇÃO — INSERIR
# =======================================================
@app.route("/movimentacao/add", methods=["POST"])
def add_evento():
    bq.insert_evento(request.form)
    return redirect("/movimentacao")


# =======================================================
# RUN
# =======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
