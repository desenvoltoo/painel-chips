# -*- coding: utf-8 -*-

from google.cloud import bigquery
import pandas as pd
import os
import uuid

# ===========================
# VARIÁVEIS DE AMBIENTE
# ===========================
PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# Função auxiliar para escapar aspas
def q(x: str):
    if x in (None, "", "None"):
        return ""
    return str(x).replace("'", "''")


class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(
            project=PROJECT,
            location=LOCATION
        )

    # ============================================================
    # EXECUTA SQL E RETORNA DATAFRAME
    # ============================================================
    def _run(self, sql: str, job_config=None):
        try:
            job = self.client.query(sql, job_config=job_config)
            return job.result().to_dataframe()
        except Exception as e:
            print("🔥 ERRO SQL:")
            print(sql)
            print(e)
            raise e

    # ============================================================
    # VIEW PRINCIPAL
    # ============================================================
    def get_view(self):
        sql = f"""
            SELECT
                sk_chip,
                id_chip,
                numero,
                operadora,
                plano,
                status,
                dt_inicio,
                ultima_recarga_data,
                ultima_recarga_valor,
                total_gasto,
                id_aparelho,
                modelo_aparelho,
                marca_aparelho,
                imei_aparelho,
                status_aparelho
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            ORDER BY numero
        """
        return self._run(sql)

    # ============================================================
    # APARELHOS
    # ============================================================
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
            ORDER BY modelo
        """
        return self._run(sql)

    def upsert_aparelho(self, form):

        id_aparelho = q(form.get("id_aparelho"))
        modelo = q(form.get("modelo"))
        marca = q(form.get("marca"))
        imei = q(form.get("imei"))
        status = q(form.get("status") or "ATIVO")

        # pega próximo SK
        sql_next = f"""
            SELECT COALESCE(MAX(sk_aparelho), 0) + 1 AS next_sk
            FROM `{PROJECT}.{DATASET}.dim_aparelho`
        """
        next_sk = int(self._run(sql_next).iloc[0]["next_sk"])

        sql = f"""
            MERGE `{PROJECT}.{DATASET}.dim_aparelho` T
            USING (SELECT '{id_aparelho}' AS id_aparelho) S
            ON T.id_aparelho = S.id_aparelho

            WHEN MATCHED THEN UPDATE SET
                modelo = '{modelo}',
                marca = '{marca}',
                imei = '{imei}',
                status = '{status}',
                updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                sk_aparelho, id_aparelho, modelo,
                marca, imei, status, ativo, created_at, updated_at
            )
            VALUES (
                {next_sk}, '{id_aparelho}', '{modelo}',
                '{marca}', '{imei}', '{status}', TRUE,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """
        self._run(sql)

    # ============================================================
    # CHIPS
    # ============================================================
    def get_chips(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            ORDER BY numero
        """
        return self._run(sql)

    def upsert_chip(self, form):

        id_chip = form.get("id_chip") or str(uuid.uuid4())
        id_chip_sql = q(id_chip)

        numero = q(form.get("numero"))
        operadora = q(form.get("operadora"))
        plano = q(form.get("plano"))
        status = q(form.get("status") or "DISPONIVEL")

        # datas
        def sql_date(x):
            return f"DATE('{x}')" if x else "NULL"

        dt_inicio = sql_date(form.get("dt_inicio"))
        dt_recarga = sql_date(form.get("ultima_recarga_data"))

        # valores numéricos
        def sql_num(x):
            try:
                if not x:
                    return 0
                return float(str(x).replace(",", "."))
            except:
                return 0

        val_recarga = sql_num(form.get("ultima_recarga_valor"))
        total_gasto = sql_num(form.get("total_gasto"))

        sk_aparelho = form.get("sk_aparelho_atual") or None
        aparelho_sql = sk_aparelho if sk_aparelho else "NULL"

        sql = f"""
            MERGE `{PROJECT}.{DATASET}.dim_chip` T
            USING (SELECT '{id_chip_sql}' AS id_chip) S
            ON T.id_chip = S.id_chip

            WHEN MATCHED THEN UPDATE SET
                numero = '{numero}',
                operadora = '{operadora}',
                plano = '{plano}',
                status = '{status}',
                dt_inicio = {dt_inicio},
                ultima_recarga_valor = {val_recarga},
                ultima_recarga_data = {dt_recarga},
                total_gasto = {total_gasto},
                sk_aparelho_atual = {aparelho_sql},
                updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                id_chip, numero, operadora, plano, status,
                dt_inicio, ultima_recarga_valor, ultima_recarga_data,
                total_gasto, sk_aparelho_atual, ativo,
                created_at, updated_at
            )
            VALUES (
                '{id_chip_sql}', '{numero}', '{operadora}', '{plano}', '{status}',
                {dt_inicio}, {val_recarga}, {dt_recarga},
                {total_gasto}, {aparelho_sql}, TRUE,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """
        self._run(sql)

    # ============================================================
    # MOVIMENTAÇÃO DO CHIP
    # ============================================================
    def registrar_movimento_chip(self, sk_chip, sk_aparelho, tipo, origem, observacao):

        query = f"""
            CALL `{PROJECT}.{DATASET}.sp_registrar_movimento_chip`(
                @sk_chip, @sk_aparelho, @tipo, @origem, @obs
            )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", sk_aparelho),
                bigquery.ScalarQueryParameter("tipo", "STRING", tipo),
                bigquery.ScalarQueryParameter("origem", "STRING", origem),
                bigquery.ScalarQueryParameter("obs", "STRING", observacao),
            ]
        )

        self.client.query(query, job_config=job_config).result()
        return True

    # ============================================================
    # EVENTOS DO CHIP
    # ============================================================
    def get_eventos(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.f_chip_aparelho`
            ORDER BY data_movimento DESC
        """
        return self._run(sql)
