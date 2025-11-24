from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client()

    # =======================
    # VIEW PRINCIPAL DO PAINEL
    # =======================
    def get_view(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY sk_chip
        """
        return self.client.query(sql).to_dataframe()

    # =======================
    # KPIs DO DASHBOARD
    # =======================
    def get_kpis(self):
        sql = f"""
        SELECT
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip`) AS total_chips,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_aparelho`) AS total_aparelhos,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.f_chip_aparelho`) AS total_eventos,
          (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip` WHERE status='ATIVO') AS chips_ativos
        """
        return self.client.query(sql).to_dataframe().iloc[0].to_dict()

    # =======================
    # ÚLTIMAS RECARGAS
    # =======================
    def get_ultimas_recargas(self):
        sql = f"""
        SELECT 
            numero,
            operadora,
            data_ultima_recarga,
            valor_ultima_recarga
        FROM `{PROJECT}.{DATASET}.dim_chip`
        WHERE data_ultima_recarga IS NOT NULL
        ORDER BY data_ultima_recarga DESC
        LIMIT 10
        """
        return self.client.query(sql).to_dataframe()

    # =======================
    # APARELHOS LISTA
    # =======================
    def get_aparelhos(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.dim_aparelho`
        ORDER BY nome_aparelho
        """
        return self.client.query(sql).to_dataframe()

    # =======================
    # CHIPS LISTA
    # =======================
    def get_chips(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.dim_chip`
        ORDER BY numero
        """
        return self.client.query(sql).to_dataframe()
