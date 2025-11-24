from google.cloud import bigquery
import pandas as pd

PROJECT = "painel-universidade"
DATASET = "painel_chips"   # ajuste aqui se seu dataset for outro!!


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ---------------------------------------
    # FUNÇÃO BASE PARA CONSULTA
    # ---------------------------------------
    def _run(self, sql):
        try:
            job = self.client.query(sql)
            return job.result().to_dataframe()
        except Exception as e:
            print("ERRO BIGQUERY:", e)
            raise e

    # ---------------------------------------
    # DASHBOARD / VIEW PRINCIPAL
    # ---------------------------------------
    def get_view(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY sk_chip
        """
        return self._run(sql)

    # ---------------------------------------
    # LISTA DE APARELHOS
    # ---------------------------------------
    def get_aparelhos(self):
        sql = f"""
        SELECT
            sk_aparelho,
            id_aparelho,
            modelo,
            marca,
            imei,
            status,
            ativo
        FROM `{PROJECT}.{DATASET}.dim_aparelho`
        ORDER BY sk_aparelho DESC
        """
        return self._run(sql)

    # ---------------------------------------
    # CADASTRAR APARELHO
    # ---------------------------------------
    def insert_aparelho(self, data):

        def clean(x): return None if x == "" else x

        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_aparelho` (
            id_aparelho,
            modelo,
            marca,
            imei,
            status,
            ativo,
            create_at,
            update_at
        )
        VALUES (
            @id_aparelho,
            @modelo,
            @marca,
            @imei,
            @status,
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """

        params = [
            bigquery.ScalarQueryParameter("id_aparelho", "STRING", clean(data.get("id_aparelho"))),
            bigquery.ScalarQueryParameter("modelo", "STRING", clean(data.get("modelo"))),
            bigquery.ScalarQueryParameter("marca", "STRING", clean(data.get("marca"))),
            bigquery.ScalarQueryParameter("imei", "STRING", clean(data.get("imei"))),
            bigquery.ScalarQueryParameter("status", "STRING", clean(data.get("status"))),
        ]

        cfg = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(sql, cfg).result()

    # ---------------------------------------
    # LISTA DE CHIPS
    # ---------------------------------------
    def get_chips(self):
        sql = f"""
        SELECT
            sk_chip,
            id_chip,
            numero,
            operadora,
            plano,
            status,
            dt_inicio,
            ultima_recarga_valor,
            ultima_recarga_data,
            total_gasto,
            sk_aparelho_atual,
            ativo
        FROM `{PROJECT}.{DATASET}.dim_chip`
        ORDER BY sk_chip DESC
        """
        return self._run(sql)

    # ---------------------------------------
    # CADASTRAR CHIP
    # ---------------------------------------
    def insert_chip(self, data):

        def clean(x):
            if x == "" or x is None:
                return None
            return x

        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_chip` (
            id_chip,
            numero,
            operadora,
            plano,
            status,
            dt_inicio,
            ultima_recarga_valor,
            ultima_recarga_data,
            total_gasto,
            sk_aparelho_atual,
            ativo,
            create_at,
            update_at
        )
        VALUES (
            @id_chip,
            @numero,
            @operadora,
            @plano,
            @status,
            @dt_inicio,
            @ultima_recarga_valor,
            @ultima_recarga_data,
            @total_gasto,
            @sk_aparelho_atual,
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """

        params = [
            bigquery.ScalarQueryParameter("id_chip", "STRING", clean(data.get("id_chip"))),
            bigquery.ScalarQueryParameter("numero", "STRING", clean(data.get("numero"))),
            bigquery.ScalarQueryParameter("operadora", "STRING", clean(data.get("operadora"))),
            bigquery.ScalarQueryParameter("plano", "STRING", clean(data.get("plano"))),
            bigquery.ScalarQueryParameter("status", "STRING", clean(data.get("status"))),
            bigquery.ScalarQueryParameter("dt_inicio", "DATE", clean(data.get("dt_inicio"))),
            bigquery.ScalarQueryParameter("ultima_recarga_valor", "NUMERIC", clean(data.get("ultima_recarga_valor"))),
            bigquery.ScalarQueryParameter("ultima_recarga_data", "DATE", clean(data.get("ultima_recarga_data"))),
            bigquery.ScalarQueryParameter("total_gasto", "NUMERIC", clean(data.get("total_gasto"))),
            bigquery.ScalarQueryParameter("sk_aparelho_atual", "INT64", clean(data.get("sk_aparelho_atual"))),
        ]

        cfg = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(sql, cfg).result()

    # ---------------------------------------
    # MOVIMENTAÇÃO
    # ---------------------------------------
    def get_eventos(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.f_chip_aparelho`
        ORDER BY data_uso DESC
        """
        return self._run(sql)

    def insert_evento(self, data):

        def clean(x): return None if x == "" else x

        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.f_chip_aparelho` (
            sk_chip,
            sk_aparelho,
            data_uso,
            tipo_movimento,
            origem,
            observacao,
            create_at,
            update_at
        )
        VALUES (
            @sk_chip,
            @sk_aparelho,
            @data_uso,
            @tipo_movimento,
            @origem,
            @observacao,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """

        params = [
            bigquery.ScalarQueryParameter("sk_chip", "INT64", clean(data.get("sk_chip"))),
            bigquery.ScalarQueryParameter("sk_aparelho", "INT64", clean(data.get("sk_aparelho"))),
            bigquery.ScalarQueryParameter("data_uso", "DATE", clean(data.get("data_uso"))),
            bigquery.ScalarQueryParameter("tipo_movimento", "STRING", clean(data.get("tipo_movimento"))),
            bigquery.ScalarQueryParameter("origem", "STRING", clean(data.get("origem"))),
            bigquery.ScalarQueryParameter("observacao", "STRING", clean(data.get("observacao"))),
        ]

        cfg = bigquery.QueryJobConfig(query_parameters=params)
        self.client.query(sql, cfg).result()
