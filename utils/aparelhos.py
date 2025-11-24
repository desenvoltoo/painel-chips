from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"
TABLE = f"{PROJECT}.{DATASET}.dim_aparelho"

client = bigquery.Client()

def listar_aparelhos():
    sql = f"SELECT * FROM `{TABLE}` ORDER BY sk_aparelho DESC"
    return list(client.query(sql).result())

def inserir_aparelho(data):
    row = {
        "nome": data.get("nome"),
        "marca": data.get("marca"),
        "modelo": data.get("modelo"),
        "imei": data.get("imei"),
        "status": data.get("status")
    }
    client.insert_rows_json(TABLE, [row])
