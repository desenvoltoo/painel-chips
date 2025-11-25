from utils.bigquery_client import BigQueryClient

def adicionar_recarga(sk_chip, valor, data, origem="painel", observacao=""):
    return registrar_recarga(sk_chip, valor, data, origem, observacao)
