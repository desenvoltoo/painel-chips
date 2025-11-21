from utils.bigquery_client import listar_chips, listar_eventos_chip

def get_chips():
    return listar_chips()

def get_chip_eventos(sk_chip):
    return listar_eventos_chip(sk_chip)
