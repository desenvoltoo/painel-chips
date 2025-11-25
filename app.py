# ============================================================
# app.py — Painel de Chips (versão corrigida final)
# ============================================================

from flask import Flask, render_template
from datetime import datetime, date
import pandas as pd

# Blueprints
from routes.chips import chips_bp
from routes.aparelhos import bp_aparelhos
from routes.movimentacao import bp_mov

# BigQuery
from utils.bigquery_client import BigQueryClient

app = Flask(__name__)
bq = BigQueryClient()


# ============================================================
# FUNÇÃO GLOBAL – SANITIZA TUDO PARA JSON (Cloud Run SAFE)
# ============================================================
def sanitize_df(df):
    """
    Corrige NaT, Timestamp, datetime e None para strings adequadas ao JSON.
    Evita erro: NaTType does not support timetuple (Cloud Run)
    """
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].replace({pd.NA: None})
        elif pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")
        else:
            df[col] = df[col].astype(str).replace("NaT", "")
    return df


# ============================================================
# ROTAS PRINCIPAIS (DASHBOARD)
# ============================================================
@app.route("/")
@app.route("/dashboard")
def dashboard():
    df = bq.get_view()

    # Sanitize para JSON seguro
    df = sanitize_df(df)

    # ===== KPIs =====
    total_chips = len(df)
    chips_ativos = len(df[df["ativo"] == True])
    disparando = len(df[df["status"] == "DISPARANDO"])
    banidos = len(df[df["status"] == "BANIDO"])

    # ===== Alerta de Recarga =====
    hoje = date.today()

    def calc_dias(x):
        if isinstance(x, str) and x.strip() != "":
            try:
                d = datetime.strptime(x, "%Y-%m-%d").date()
                return (hoje - d).days
            except:
                return 999
        return 999

    df["dias_sem_recarga"] = df["ultima_recarga_data"].apply(calc_dias)
    alerta_recarga = df[df["dias_sem_recarga"] >= 80]

    # ===== Filtros =====
    lista_status = sorted(df["status"].unique())
    lista_operadora = sorted(df["operadora"].unique())

    df["aparelho_label"] = (
        df["marca_aparelho"].fillna("") + " " +
        df["modelo_aparelho"].fillna("")
    ).str.strip()

    lista_aparelho = sorted(x for x in df["aparelho_label"].unique() if x != "")

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


# ============================================================
# BLUEPRINTS (Chips / Aparelhos / Movimentação)
# ============================================================
app.register_blueprint(chips_bp)
app.register_blueprint(bp_aparelhos)
app.register_blueprint(bp_mov)


# ============================================================
# RUN LOCAL
# ============================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
