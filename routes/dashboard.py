from flask import Blueprint, render_template
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

bp_dashboard = Blueprint("dashboard", __name__)

PROJECT_ID = ["GCP_PROJECT_ID"]
DATASET = ["BQ_DATASET"]

bq = BigQueryClient()

@bp_dashboard.route("/")
@bp_dashboard.route("/dashboard")
def dashboard():

    df = bq.get_view("vw_chips_painel")
    df = sanitize_df(df)
    tabela = df.to_dict(orient="records")

    total_chips = len(tabela)
    chips_ativos = sum(1 for x in tabela if (x["status"] or "").upper() == "ATIVO")
    disparando = sum(1 for x in tabela if (x["status"] or "").upper() == "DISPARANDO")
    banidos = sum(1 for x in tabela if (x["status"] or "").upper() == "BANIDO")

    lista_status = sorted({ (x["status"] or "").upper() for x in tabela if x["status"] })
    lista_operadora = sorted({ x["operadora"] for x in tabela if x["operadora"] })

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
        qtd_alerta=len(alerta)
    )

