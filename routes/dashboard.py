# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
import os

from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df


# ===============================================================
# BLUEPRINT
# ===============================================================
bp_dashboard = Blueprint("dashboard", __name__)


# ===============================================================
# CONFIGURAÇÕES
# ===============================================================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")

bq = BigQueryClient()


# ===============================================================
# ROTA DO DASHBOARD
# ===============================================================
@bp_dashboard.route("/")
@bp_dashboard.route("/dashboard")
def dashboard():

    # ----- 1) CARREGA VIEW -----
    df = bq.get_view("vw_chips_painel")
    df = sanitize_df(df)
    tabela = df.to_dict(orient="records")

    # ----- 2) KPIs -----
    total_chips = len(tabela)
    chips_ativos = sum(1 for x in tabela if (x.get("status") or "").upper() == "ATIVO")
    disparando = sum(1 for x in tabela if (x.get("status") or "").upper() == "DISPARANDO")
    banidos = sum(1 for x in tabela if (x.get("status") or "").upper() == "BANIDO")

    # ----- 3) FILTROS / GRÁFICOS -----
    lista_status = sorted({ (x.get("status") or "").upper() for x in tabela if x.get("status") })
    lista_operadora = sorted({ x.get("operadora") for x in tabela if x.get("operadora") })

    # ----- 4) ALERTAS (chips > 80 dias sem recarga) -----
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

    # ----- 5) RETORNO -----
    return render_template(
        "dashboard.html",
        tabela=tabela,

        # KPIs
        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,

        # filtros
        lista_status=lista_status,
        lista_operadora=lista_operadora,

        # alertas
        alerta_recarga=alerta,
        qtd_alerta=len(alerta),
    )
