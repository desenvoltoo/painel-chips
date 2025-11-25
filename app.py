# app.py
from flask import Flask, render_template
from routes.chips import chips_bp
from routes.aparelhos import bp_aparelhos
from routes.movimentacao import bp_mov
from utils.bigquery_client import BigQueryClient
from datetime import datetime, date
import pandas as pd

app = Flask(__name__)
bq = BigQueryClient()


# =======================================================
# FUNÇÃO UNIVERSAL DE NORMALIZAÇÃO (EVITA ERRO 500)
# =======================================================
def safe_normalize(df):
    """
    Remove NaT/None e converte datas para string,
    garantindo que o tojson funcione no Cloud Run.
    """
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str).replace("NaT", "")
        elif pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")
        else:
            df[col] = df[col].apply(lambda x: x if x is not None else "")
    return df


# =======================================================
# ROTAS GLOBAIS — DASHBOARD
# =======================================================
@app.route("/")
@app.route("/dashboard")
def dashboard():
    df = bq.get_view()
    df = safe_normalize(df)

    # ============ KPIs ============
    total_chips = len(df)
    chips_ativos = len(df[df["ativo"] == True])
    disparando = len(df[df["status"] == "DISPARANDO"])
    banidos = len(df[df["status"] == "BANIDO"])

    # ============ ALERTA ============
    hoje = date.today()

    def calc_dias(x):
        try:
            if x in ("", None):
                return 999
            x = pd.to_datetime(x).date()
            return (hoje - x).days
        except:
            return 999

    df["dias_sem_recarga"] = df["ultima_recarga_data"].apply(calc_dias)

    alerta_recarga = df[df["dias_sem_recarga"] >= 80]

    # ============ FILTROS ============
    lista_status = sorted(df["status"].unique())
    lista_operadora = sorted(df["operadora"].unique())

    df["aparelho_label"] = df.apply(
        lambda x: f"{x.get('marca_aparelho','')} {x.get('modelo_aparelho','')}".strip(),
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
# BLUEPRINTS
# =======================================================
app.register_blueprint(chips_bp)
app.register_blueprint(bp_aparelhos)
app.register_blueprint(bp_mov)


# =======================================================
# RUN
# =======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
