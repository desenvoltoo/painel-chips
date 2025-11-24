from google.cloud import bigquery
from datetime import datetime

PROJECT = "painel-universidade"
DATASET = "marts"
TABLE = f"{PROJECT}.{DATASET}.f_chip_aparelho"

client = bigquery.Client()

def listar_eventos():
    sql = f"""
    SELECT *
    FROM `{TABLE}`
    ORDER BY data_uso DESC
    """
    return list(client.query(sql).result())

def registrar_evento(data):
    row = {
        "sk_chip": int(data.get("sk_chip")),
        "sk_aparelho": int(data.get("sk_aparelho")),
        "tipo_movimento": data.get("tipo_movimento"),
        "data_uso": data.get("data_uso"),
        "origem": data.get("origem"),
        "observacao": data.get("observacao"),
    }
    client.insert_rows_json(TABLE, [row])
