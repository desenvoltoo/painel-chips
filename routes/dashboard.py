# routes/dashboard.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
import os

from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

# ===============================================================
# BLUEPRINT
# ===============================================================
bp_dashboard = Blueprint("dashboard", __name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET    = os.getenv("BQ_DATASET", "marts")

bq = BigQueryClient()

# ===============================================================
# DASHBOARD
# ===============================================================
@bp_dashboard.route("/")
@bp_dashboard.route("/dashboard")
def dashboard():

    # ===========================================================
    # 1) TABELA PRINCIPAL — CHIP + APARELHO ATUAL
    # ===========================================================
    sql = f"""
        SELECT
            c.sk_chip,
            c.numero,
            c.operadora,
            c.operador,
            c.status,
            c.ultima_recarga_data,
            c.total_gasto,
            c.sk_aparelho_atual,

            a.marca  AS aparelho_marca,
            a.modelo AS aparelho_modelo

        FROM `{PROJECT_ID}.{DATASET}.vw_chips_painel_base` c
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_aparelho` a
            ON a.sk_aparelho = c.sk_aparelho_atual
        ORDER BY c.numero
    """

    df = sanitize_df(bq.run_df(sql))
    tabela = df.to_dict(orient="records")

    # Campo calculado esperado pelo HTML
    for r in tabela:
        if r.get("aparelho_marca") and r.get("aparelho_modelo"):
            r["aparelho"] = f"{r['aparelho_marca']} • {r['aparelho_modelo']}"
        else:
            r["aparelho"] = "—"

    # ===========================================================
    # 2) KPIs
    # ===========================================================
    total_chips = len(tabela)

    chips_ativos = sum(
        1 for x in tabela if (x.get("status") or "").upper() == "ATIVO"
    )
    disparando = sum(
        1 for x in tabela if (x.get("status") or "").upper() == "DISPARANDO"
    )
    banidos = sum(
        1 for x in tabela if (x.get("status") or "").upper() == "BANIDO"
    )

    # ===========================================================
    # 3) FILTROS
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

    lista_operadores = sorted({
        x.get("operador")
        for x in tabela
        if x.get("operador")
    })

    # ===========================================================
    # 4) ALERTAS — > 80 DIAS SEM RECARGA (CORRIGIDO)
    # ===========================================================
    alerta_sql = f"""
        SELECT
            c.numero,
            c.status,
            c.operadora,
            c.sk_aparelho_atual,

            a.marca  AS aparelho_marca,
            a.modelo AS aparelho_modelo,

            c.ultima_recarga_data,
            DATE_DIFF(
                CURRENT_DATE(),
                DATE(c.ultima_recarga_data),
                DAY
            ) AS dias_sem_recarga

        FROM `{PROJECT_ID}.{DATASET}.vw_chips_painel_base` c
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_aparelho` a
            ON a.sk_aparelho = c.sk_aparelho_atual
        WHERE c.ultima_recarga_data IS NOT NULL
          AND DATE_DIFF(
                CURRENT_DATE(),
                DATE(c.ultima_recarga_data),
                DAY
          ) > 80
        ORDER BY dias_sem_recarga DESC
    """

    alerta_df = sanitize_df(bq.run_df(alerta_sql))
    alerta = alerta_df.to_dict(orient="records")

    for r in alerta:
        if r.get("aparelho_marca") and r.get("aparelho_modelo"):
            r["aparelho"] = f"{r['aparelho_marca']} • {r['aparelho_modelo']}"
        else:
            r["aparelho"] = "—"

    # ===========================================================
    # 5) RENDER
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
        lista_operadores=lista_operadores,

        alerta_recarga=alerta,
        qtd_alerta=len(alerta),
    )
