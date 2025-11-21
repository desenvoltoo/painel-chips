from utils.bigquery_client import get_bq_client

def listar_aparelhos():
    sql = "SELECT id_aparelho, marca, modelo, tipo, status FROM `painel-universidade.marts.dim_aparelho` WHERE status='ativo' ORDER BY marca, modelo"
    return list(get_bq_client().query(sql).result())
