# ============================================================
# app.py — Painel de Chips (versão limpa e funcional)
# ============================================================

from flask import Flask, render_template
from datetime import datetime, date
import pandas as pd

# Blueprints — apenas os que realmente existem
from utils.chips import chips_bp
from utils.aparelhos import bp_aparelhos

# BigQuery
from utils.bigquery_client import BigQueryClient

app = Flask(__name__)
bq = BigQueryClient()


# ============================================================
# FUNÇÃO GLOBAL – SANITIZAÇÃO PARA JSON (Cloud Run SAFE)
# ============================================================
def sanitize_df(df):
    for col in df.columns:

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d").fillna("")

        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].fillna(0).apply(lambda x: f"{x:.2f}")

        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].replace({pd.NA: None})

        elif pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")

        else:
            df[col] = df[col].astype(str).replace("NaT", "")

    return df

# ============================================================
# ROTA PRINCIPAL — DASHBOARD
# ============================================================
@app.route("/")
@app.route("/dashboard")
def dashboard():
    df = bq.get_view()

    total_chips = len(df)
    chips_ativos = len(df[df["status"] == "ATIVO"])
    disparando = len(df[df["status"] == "DISPARANDO"])
    banidos = len(df[df["status"] == "BANIDO"])

    # ALERTA de 80 dias
    alerta = df.copy()
    alerta["dias_sem_recarga"] = (
        pd.Timestamp.now() - pd.to_datetime(alerta["ultima_recarga_data"])
    ).dt.days

    alerta_recarga = alerta[alerta["dias_sem_recarga"] > 80]
    qtd_alerta = len(alerta_recarga)

    lista_status = sorted(df["status"].fillna("").str.upper().unique())
    lista_operadora = sorted(df["operadora"].dropna().unique())

    return render_template(
        "dashboard.html",
        tabela=df.to_dict(orient="records"),
        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,
        alerta_recarga=alerta_recarga.to_dict(orient="records"),
        qtd_alerta=qtd_alerta,
        lista_status=lista_status,
        lista_operadora=lista_operadora
    )

# ============================================================
# BLUEPRINTS — APENAS OS QUE EXISTEM
# ============================================================
app.register_blueprint(chips_bp)
app.register_blueprint(bp_aparelhos)


# ============================================================
# RUN LOCAL
# ============================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
