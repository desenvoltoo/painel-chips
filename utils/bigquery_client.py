from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client()

    # ==============================
    # DASHBOARD KPIs
    # ==============================
    def kpis_dashboard(self):
        sql = f"""
        SELECT 
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip`) AS total_chips,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip` WHERE status_chip='ATIVO') AS chips_ativos,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_aparelho`) AS total_aparelhos,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.f_chip_aparelho` WHERE data_desvinculo IS NULL) AS aparelhos_vinculados
        """
        return self.client.query(sql).result().to_dataframe().iloc[0].to_dict()

    # ==============================
    # ÚLTIMAS RECARGAS
    # ==============================
    def get_ultimas_recargas(self):
        sql = f"""
        SELECT numero_chip, operadora, data_ultima_recarga, valor_ultima_recarga
        FROM `{PROJECT}.{DATASET}.dim_chip`
        WHERE data_ultima_recarga IS NOT NULL
        ORDER BY data_ultima_recarga DESC
        LIMIT 10
        """
        return list(self.client.query(sql).result())

    # ==============================
    # VIEW CONSOLIDADA
    # ==============================
    def get_view_painel(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY sk_chip
        """
        return list(self.client.query(sql).result())
