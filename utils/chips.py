from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"
TABLE = f"{PROJECT}.{DATASET}.dim_chip"

client = bigquery.Client()

def listar_chips():
    sql = f"SELECT * FROM `{TABLE}` ORDER BY sk_chip DESC"
    return list(client.query(sql).result())

def inserir_chip(data):
    row = {
        "id_chip": data.get("id_chip"),
        "numero": data.get("numero"),
        "operadora": data.get("operadora"),
        "plano": data.get("plano"),
        "status": data.get("status"),
        "dt_inicio": data.get("dt_inicio"),
        "ultima_recarga_valor": float(data.get("ultima_recarga_valor") or 0),
        "ultima_recarga_data": data.get("ultima_recarga_data"),
        "total_gasto": float(data.get("total_gasto") or 0),
        "sk_aparelho_atual": int(data.get("sk_aparelho_atual") or 0),
        "ativo": True
    }

    client.insert_rows_json(TABLE, [row])
