from google.cloud import bigquery
import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = "marts"

client = bigquery.Client(project=PROJECT_ID)


# ========================
# FUNÇÕES DE CONSULTA
# ========================

def listar_chips():
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.dim_chip`
        ORDER BY numero
    """
    return client.query(query).to_dataframe().to_dict("records")


def listar_aparelhos():
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.dim_aparelho`
        ORDER BY modelo
    """
    return client.query(query).to_dataframe().to_dict("records")


def listar_eventos_chip(sk_chip):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.vw_chips_painel`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """
    return client.query(query).to_dataframe().to_dict("records")


# ========================
# INSERIR EVENTOS
# ========================

def registrar_recarga(sk_chip, valor, data, origem, observacao):
    query = f"""
        CALL `{PROJECT_ID}.{DATASET}.sp_registrar_recarga`(
            {sk_chip},
            {valor},
            DATE("{data}"),
            "{origem}",
            "{observacao}"
        );
    """
    client.query(query).result()
    return True


def vincular_chip_aparelho(sk_chip, sk_aparelho, data, origem, obs):
    query = f"""
        CALL `{PROJECT_ID}.{DATASET}.sp_vincular_chip_aparelho`(
            {sk_chip},
            {sk_aparelho},
            DATE("{data}"),
            "{origem}",
            "{obs}"
        );
    """
    client.query(query).result()
    return True


def desvincular_chip(sk_chip, data, origem, obs):
    query = f"""
        CALL `{PROJECT_ID}.{DATASET}.sp_desvincular_chip`(
            {sk_chip},
            DATE("{data}"),
            "{origem}",
            "{obs}"
        );
    """
    client.query(query).result()
    return True
