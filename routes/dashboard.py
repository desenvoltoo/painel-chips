# routes/dashboard.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
import os

from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

bp_dashboard = Blueprint("dashboard", __name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")

bq = BigQueryClient()


@bp_dashboard.route("/")
@bp_dashboard.route("/dashboard")
def dashboard():

    # ===========================================================
    # QUERY PRINCIPAL — CONTRATO 100% COMPATÍVEL COM O HTML
    # ===========================================================
    sql = f"""
        SELECT
            c.numero,
            c.operadora,
            c.plano,
            c.status,
            c.ultima_recarga_data,

            -- CAMPOS QUE O HTML ESPERA 👇
            a.marca  AS marca_aparelho,
            a.modelo AS modelo_aparelho

        FROM `{PROJECT_ID}.{DATASET}.vw_chips_painel_base` c
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_aparelho` a
            ON a.sk_aparelho = c.sk_aparelho_atual
        ORDER BY c.numero
    """

    df = sanitize_df(bq.run_df(sql))
    tabela = df.to_dict(orient="records")

    # ===========================================================
    # KPIs
    # ===========================================================
    total_chips = len(tabela)

    chips_ativos = sum(
        1 for x in tabela
        if (x.get("status") or "").upper() == "ATIVO"
    )

    disparando = sum(
        1 for x in tabela
        if (x.get("status") or "").upper() == "DISPARANDO"
    )

    banidos = sum(
        1 for x in tabela
        if (x.get("status") or "").upper() == "BANIDO"
    )

    # ===========================================================
    # FILTROS
    # ===========================================================
    lista_status = sorted({
        (x.get("status") or "").upper()
        for x in tabela
        if x.get("status")
    })

    lista_operadora = sorted({
        x.get("operadora")
        for x in tabela
        if x.get("operadora")
    })

    # ===========================================================
    # ALERTAS — > 80 DIAS SEM RECARGA
    # ===========================================================
    alerta_sql = f"""
        SELECT
            c.numero,
            c.status,
            c.operadora,
            c.ultima_recarga_data,
            DATE_DIFF(
                CURRENT_DATE(),
                DATE(c.ultima_recarga_data),
                DAY
            ) AS dias_sem_recarga
        FROM `{PROJECT_ID}.{DATASET}.vw_chips_painel_base` c
        WHERE c.ultima_recarga_data IS NOT NULL
          AND DATE_DIFF(
                CURRENT_DATE(),
                DATE(c.ultima_recarga_data),
                DAY
          ) > 80
        ORDER BY dias_sem_recarga DESC
    """

    alerta_df = sanitize_df(bq.run_df(alerta_sql))
    alerta_recarga = alerta_df.to_dict(orient="records")

    # ===========================================================
    # RENDER
    # ===========================================================
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
