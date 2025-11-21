from utils.bigquery_client import registrar_recarga

def adicionar_recarga(sk_chip, valor, data, origem="painel", observacao=""):
    return registrar_recarga(sk_chip, valor, data, origem, observacao)
