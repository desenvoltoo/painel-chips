from google.cloud import bigquery
import pandas as pd

PROJECT = "painel-universidade"
DATASET = "marts"


class BigQueryClient:
    def __init__(self):
        # Cliente único — evita reconexões desnecessárias
        self.client = bigquery.Client(project=PROJECT)

    # =======================
    # VIEW PRINCIPAL DO PAINEL
    # =======================
    def get_view(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            ORDER BY sk_chip
        """
        return self._run_query(sql)

    # =======================
    # KPIs DO DASHBOARD
    # =======================
    def get_kpis(self):
        sql = f"""
            SELECT
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip`) AS total_chips,
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_aparelho`) AS total_aparelhos,
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.f_chip_aparelho`) AS total_eventos,
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip`
                    WHERE status = 'ATIVO') AS chips_ativos
        """

        df = self._run_query(sql)
        return df.iloc[0].to_dict()

    # =======================
    # ÚLTIMAS RECARGAS
    # =======================
    def get_ultimas_recargas(self):
        sql = f"""
            SELECT 
                numero,
                operadora,
                ultima_recarga_data,
                ultima_recarga_valor
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            WHERE ultima_recarga_data IS NOT NULL
            ORDER BY ultima_recarga_data DESC
            LIMIT 10
        """
        return self._run_query(sql)

    # =======================
    # LISTA DE APARELHOS
    # =======================
    def get_aparelhos(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_aparelho`
            ORDER BY nome_aparelho
        """
        return self._run_query(sql)

    # =======================
    # LISTA DE CHIPS
    # =======================
    def get_chips(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_chip`
            ORDER BY numero
        """
        return self._run_query(sql)

    # =======================
    # Execução centralizada de queries
    # =======================
    def _run_query(self, sql: str) -> pd.DataFrame:
        try:
            job = self.client.query(sql)
            return job.to_dataframe()  # usa pandas + db-dtypes
        except Exception as e:
            print(f"[ERRO QUERY]\nSQL: {sql}\nErro: {e}")
            raise e
