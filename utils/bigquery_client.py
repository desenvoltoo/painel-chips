# utils/bigquery_client.py
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "painel-universidade"
DATASET = "chips_db"

client = bigquery.Client(project=PROJECT_ID)

# -------------------------
# HELPERS
# -------------------------
def run_query(query, params=None):
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params

    query_job = client.query(query, job_config=job_config)
    return [dict(row) for row in query_job]

def run_dml(query, params=None):
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params

    client.query(query, job_config=job_config).result()
    return True


# =====================================================
# CHIPS
# =====================================================
def listar_chips():
    query = f"""
        SELECT 
            id_chip,
            numero,
            operadora,
            status_ativo,
            valor_ultima_recarga,
            data_ultima_recarga
        FROM `{PROJECT_ID}.{DATASET}.dim_chip`
        ORDER BY numero
    """
    return run_query(query)


def salvar_chip(data):
    query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.dim_chip` T
        USING (SELECT @id_chip AS id_chip) S
        ON T.id_chip = S.id_chip
        WHEN MATCHED THEN
            UPDATE SET 
                numero = @numero,
                operadora = @operadora,
                status_ativo = @status_ativo
        WHEN NOT MATCHED THEN
            INSERT (id_chip, numero, operadora, status_ativo)
            VALUES (@id_chip, @numero, @operadora, @status_ativo)
    """

    params = [
        bigquery.ScalarQueryParameter("id_chip", "INT64", data.get("id_chip")),
        bigquery.ScalarQueryParameter("numero", "STRING", data.get("numero")),
        bigquery.ScalarQueryParameter("operadora", "STRING", data.get("operadora")),
        bigquery.ScalarQueryParameter("status_ativo", "BOOL", True),
    ]

    return run_dml(query, params)


# =====================================================
# APARELHOS
# =====================================================
def listar_aparelhos():
    query = f"""
        SELECT A.id_aparelho,
               A.descricao,
               A.modelo,
               A.status,
               A.imei,
               C.numero AS chip_numero
        FROM `{PROJECT_ID}.{DATASET}.dim_aparelho` A
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_chip` C
          ON A.id_chip = C.id_chip
        ORDER BY A.descricao
    """
    return run_query(query)


def salvar_aparelho(data):
    query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.dim_aparelho` T
        USING (SELECT @id_aparelho AS id_aparelho) S
        ON T.id_aparelho = S.id_aparelho
        WHEN MATCHED THEN
            UPDATE SET 
                descricao = @descricao,
                modelo = @modelo,
                status = @status,
                imei = @imei,
                id_chip = @id_chip
        WHEN NOT MATCHED THEN
            INSERT (id_aparelho, descricao, modelo, status, imei, id_chip)
            VALUES (@id_aparelho, @descricao, @modelo, @status, @imei, @id_chip)
    """

    params = [
        bigquery.ScalarQueryParameter("id_aparelho", "INT64", data.get("id_aparelho")),
        bigquery.ScalarQueryParameter("descricao", "STRING", data.get("descricao")),
        bigquery.ScalarQueryParameter("modelo", "STRING", data.get("modelo")),
        bigquery.ScalarQueryParameter("status", "STRING", data.get("status")),
        bigquery.ScalarQueryParameter("imei", "STRING", data.get("imei")),
        bigquery.ScalarQueryParameter("id_chip", "INT64", data.get("chip_id")),
    ]

    return run_dml(query, params)


# =====================================================
# RECARGAS
# =====================================================
def listar_recargas():
    query = f"""
        SELECT 
            R.id_recarga,
            C.numero,
            C.operadora,
            R.valor,
            R.data,
            R.obs
        FROM `{PROJECT_ID}.{DATASET}.f_chip_recarga` R
        JOIN `{PROJECT_ID}.{DATASET}.dim_chip` C
          ON R.id_chip = C.id_chip
        ORDER BY R.data DESC
    """
    return run_query(query)


def salvar_recarga(data):
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.f_chip_recarga`
            (id_chip, valor, data, obs)
        VALUES (@id_chip, @valor, @data, @obs)
    """

    params = [
        bigquery.ScalarQueryParameter("id_chip", "INT64", data.get("id_chip")),
        bigquery.ScalarQueryParameter("valor", "FLOAT64", float(data.get("valor"))),
        bigquery.ScalarQueryParameter("data", "DATE", data.get("data")),
        bigquery.ScalarQueryParameter("obs", "STRING", data.get("obs")),
    ]

    return run_dml(query, params)


# =====================================================
# DASHBOARD
# =====================================================
def dashboard_resumo():
    query = f"""
        SELECT 
            (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.dim_chip`) AS total_chips,
            (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET}.dim_aparelho`) AS total_aparelhos,
            (SELECT SUM(valor) FROM `{PROJECT_ID}.{DATASET}.f_chip_recarga`) AS total_recargas
    """
    return run_query(query)[0]
