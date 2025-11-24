from google.cloud import bigquery
import datetime

PROJECT = "painel-universidade"
DATASET = "marts"
TABLE = f"{PROJECT}.{DATASET}.f_chip_aparelho"

client = bigquery.Client()

def listar_relacionamentos():
    sql = f"""
    SELECT *
    FROM `{TABLE}`
    ORDER BY data_vinculo DESC
    """
    return list(client.query(sql).result())

def vincular_chip(sk_chip, sk_aparelho):
    rows = [{
        "sk_chip": int(sk_chip),
        "sk_aparelho": int(sk_aparelho),
        "data_vinculo": datetime.datetime.utcnow().isoformat()
    }]
    client.insert_rows_json(TABLE, rows)

def desvincular_chip(id_rel):
    sql = f"""
    UPDATE `{TABLE}`
    SET data_desvinculo = CURRENT_TIMESTAMP()
    WHERE id = {id_rel}
    """
    client.query(sql).result()
