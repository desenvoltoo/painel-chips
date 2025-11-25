# app.py
from flask import Flask, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from datetime import datetime, date
import pandas as pd

app = Flask(__name__)
bq = BigQueryClient()


# =======================================================
# TRATAMENTO SAFE PARA JSON (EVITA ERRO DE NaT)
# =======================================================
def safe_fillna_strings(df):
    """Remove NaT das colunas DATE/DATETIME e string."""
    for col in df.columns:

        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")

        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str).replace("NaT", "")

        else:
            df[col] = df[col]

    return df


# =======================================================
# DASHBOARD
# =======================================================
@app.route("/")
@app.route("/dashboard")
def dashboard():
    df = bq.get_view()
    df = safe_fillna_strings(df)

    # KPIs
    total_chips = len(df)
    chips_ativos = len(df[df["ativo"] == True])
    disparando = len(df[df["status"] == "DISPARANDO"])
    banidos = len(df[df["status"] == "BANIDO"])

    # ALERTA
    hoje = datetime.now().date()
    df["dias_sem_recarga"] = df["ultima_recarga_data"].apply(
        lambda x: (hoje - datetime.strptime(x, "%Y-%m-%d").date()).days
        if isinstance(x, str) and x.strip() != ""
        else 999
    )

    alerta_recarga = df[df["dias_sem_recarga"] >= 80]

    # FILTROS
    lista_status = sorted(df["status"].unique())
    lista_operadora = sorted(df["operadora"].unique())

    df["aparelho_label"] = df.apply(
        lambda x: f"{x.get('marca_aparelho', '')} {x.get('modelo_aparelho', '')}".strip(),
        axis=1
    )

    lista_aparelho = sorted(df["aparelho_label"].unique())

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
# APARELHOS
# =======================================================
@app.route("/aparelhos")
def aparelhos():
    aparelhos_df = bq.get_aparelhos()
    aparelhos_df = safe_fillna_strings(aparelhos_df)

    return render_template(
        "aparelhos.html",
        aparelhos=aparelhos_df.to_dict(orient="records"),
    )


@app.route("/aparelhos/add", methods=["POST"])
def add_aparelho():
    bq.upsert_aparelho(request.form)
    return redirect("/aparelhos")


# =======================================================
# CHIPS
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


@app.route("/chips/add", methods=["POST"])
def add_chip():
    bq.upsert_chip(request.form)
    return redirect("/chips")


# =======================================================
# MOVIMENTAÇÃO
# =======================================================
@app.route("/movimentacao")
def movimentacao():
    eventos_df = bq.get_eventos()
    eventos_df = safe_fillna_strings(eventos_df)

    return render_template(
        "movimentacao.html",
        eventos=eventos_df.to_dict(orient="records"),
    )


@app.route("/movimentacao/add", methods=["POST"])
def add_evento():
    bq.insert_evento(request.form)
    return redirect("/movimentacao")


# =======================================================
# RUN
# =======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
