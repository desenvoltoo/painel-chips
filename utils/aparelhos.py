from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"
TABLE = f"{PROJECT}.{DATASET}.dim_aparelho"

client = bigquery.Client()

def listar_aparelhos():
    sql = f"SELECT * FROM `{TABLE}` ORDER BY sk_aparelho DESC"
    return list(client.query(sql).result())

def inserir_aparelho(data):
    rows = [{
        "nome_aparelho": data.get("nome_aparelho"),
        "imei": data.get("imei"),
        "modelo": data.get("modelo"),
        "status_aparelho": data.get("status_aparelho")
    }]
    client.insert_rows_json(TABLE, rows)
