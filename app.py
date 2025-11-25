# app.py
from flask import Flask, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from datetime import datetime, date
import pandas as pd

app = Flask(__name__)
bq = BigQueryClient()


# =======================================================
# TRATAMENTO SEGURO PARA JSON (EVITA NaT e datetime)
# =======================================================
def safe_fillna_strings(df):
    """Converte somente strings, e troca datetime/NaT por string segura."""

    for col in df.columns:

        # Colunas STRING → preencher vazios
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")

        # Colunas de data → converter para string (ISO)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str).replace("NaT", "")

        # Colunas object que podem ter datetime.date misturado
        elif df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: x.isoformat() if isinstance(x, (datetime, date)) else ("" if pd.isna(x) else x)
            )

    return df


# =======================================================
# DASHBOARD
# =======================================================
@app.route("/")
@app.route("/dashboard")
def dashboard():

    df = bq.get_view()
    df = safe_fillna_strings(df)

    # ================= KPIS =================
    total_chips = len(df)
    chips_ativos = len(df[df["ativo"] == True])
    disparando = len(df[df["status"] == "DISPARANDO"])
    banidos = len(df[df["status"] == "BANIDO"])

    # ================= ALERTA RECARGA =================
    hoje = datetime.now().date()

    def calc_dias(x):
        if isinstance(x, str) and len(x) >= 8:
            try:
                d = datetime.strptime(x, "%Y-%m-%d").date()
                return (hoje - d).days
            except:
                return 999
        return 999

    df["dias_sem_recarga"] = df["ultima_recarga_data"].apply(calc_dias)
    alerta_recarga = df[df["dias_sem_recarga"] >= 80]

    # ================= FILTROS =================
    lista_status = sorted(df["status"].dropna().unique())
    lista_operadora = sorted(df["operadora"].dropna().unique())

    # Marca + modelo (com fallback)
    df["aparelho_label"] = df.apply(
        lambda x: f"{x.get('marca_aparelho','')} {x.get('modelo_aparelho','')}".strip(),
        axis=1,
    )
    lista_aparelho = sorted([a for a in df["aparelho_label"].unique() if a])

    # ================= RENDER =================
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

    df = bq.get_aparelhos()
    df = safe_fillna_strings(df)

    return render_template("aparelhos.html", aparelhos=df.to_dict(orient="records"))


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
    chips_df = safe_fillna_strings(chips_df)

    aparelhos_df = bq.get_aparelhos()
    aparelhos_df = safe_fillna_strings(aparelhos_df)

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

    df = bq.get_eventos()
    df = safe_fillna_strings(df)

    return render_template("movimentacao.html", eventos=df.to_dict(orient="records"))


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
