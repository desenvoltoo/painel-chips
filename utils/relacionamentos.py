from utils.bigquery_client import (
    vincular_chip_aparelho,
    desvincular_chip
)

def vincular(sk_chip, sk_aparelho, data, origem="painel", obs=""):
    return vincular_chip_aparelho(sk_chip, sk_aparelho, data, origem, obs)

def desvincular(sk_chip, data, origem="painel", obs=""):
    return desvincular_chip(sk_chip, data, origem, obs)
