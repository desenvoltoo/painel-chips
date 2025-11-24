from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"
TABLE = f"{PROJECT}.{DATASET}.dim_chip"

client = bigquery.Client()

def listar_chips():
    sql = f"SELECT * FROM `{TABLE}` ORDER BY sk_chip DESC"
    return list(client.query(sql).result())

def inserir_chip(data):
    rows = [{
        "iccid": data.get("iccid"),
        "numero_chip": data.get("numero_chip"),
        "operadora": data.get("operadora"),
        "status_chip": data.get("status_chip"),
        "data_ativacao": data.get("data_ativacao"),
        "data_ultima_recarga": data.get("data_ultima_recarga"),
        "valor_ultima_recarga": float(data.get("valor_ultima_recarga") or 0)
    }]

    client.insert_rows_json(TABLE, rows)
