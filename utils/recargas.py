from utils.bigquery_client import get_bq_client

def listar_recargas(id_chip):
    sql = f"SELECT * FROM `painel-universidade.marts.f_recarga` WHERE id_chip={id_chip} ORDER BY data_recarga DESC"
    return list(get_bq_client().query(sql).result())
