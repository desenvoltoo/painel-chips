from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client()

    def get_view(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY sk_chip
        """
        return list(self.client.query(sql).result())

    def get_kpis(self):
        sql = f"""
        SELECT
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip`) AS total_chips,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_aparelho`) AS total_aparelhos,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.f_chip_aparelho`) AS total_eventos
        """
        return self.client.query(sql).result().to_dataframe().iloc[0].to_dict()
