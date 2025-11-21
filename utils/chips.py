from utils.bigquery_client import get_bq_client

def listar_chips():
    sql = "SELECT * FROM `painel-universidade.marts.vw_chips_painel` ORDER BY data_update DESC"
    return list(get_bq_client().query(sql).result())
