# -*- coding: utf-8 -*-

import os
import uuid
import pandas as pd
from google.cloud import bigquery

# ===========================
# VARIÁVEIS DE AMBIENTE
# ===========================
PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# SANITIZADOR DE STRINGS PARA SQL
def q(value: str):
    if value in (None, "", "None"):
        return ""
    return str(value).replace("'", "''")


class BigQueryClient:

    def __init__(self):
        self.project = PROJECT
        self.dataset = DATASET

        self.client = bigquery.Client(
            project=PROJECT,
            location=LOCATION
        )

    # ============================================================
    # EXECUTA SQL E RETORNA DATAFRAME
    # ============================================================
    def _run(self, sql: str):

        print("\n🔥 SQL EXECUTANDO:\n", sql, "\n========================================")

        try:
            job = self.client.query(sql)
            df = job.result().to_dataframe(create_bqstorage_client=False)

            df = df.astype(object).where(pd.notnull(df), None)

            return df

        except Exception as e:
            print("\n🚨 ERRO NO SQL:\n", sql)
            raise e

    # ============================================================
    # GET VIEW
    # ============================================================
    def get_view(self, view_name: str):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.{view_name}`
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
        return self.get_view("vw_chips_painel")

    def upsert_chip(self, form):
        """
        Aceita dados de formulário normal ou JSON (modal).
        """

        # ID do chip
        id_chip = form.get("id_chip") or str(uuid.uuid4())
        id_chip_sql = q(id_chip)

        # Strings
        numero = q(form.get("numero"))
        operadora = q(form.get("operadora"))
        operador = q(form.get("operador"))
        plano = q(form.get("plano"))
        status = q(form.get("status") or "DISPONIVEL")

        # Observação
        obs_raw = form.get("observacao")
        observacao_sql = f"'{q(obs_raw)}'" if obs_raw and str(obs_raw).strip() else "NULL"

        # Datas
        def sql_date(x):
            return f"DATE('{x}')" if x and x not in ("None", "") else "NULL"

        dt_inicio = sql_date(form.get("dt_inicio"))
        dt_recarga = sql_date(form.get("ultima_recarga_data"))

        # Números
        def sql_num(x):
            if x in (None, "", "None"):
                return "NULL"
            try:
                return str(float(str(x).replace(",", ".")))
            except:
                return "NULL"

        val_recarga = sql_num(form.get("ultima_recarga_valor"))
        total_gasto = sql_num(form.get("total_gasto"))

        # FK aparelho
        sk_ap = form.get("sk_aparelho_atual")
        aparelho_sql = sk_ap if sk_ap not in (None, "", "None") else "NULL"

        sql = f"""
            MERGE `{PROJECT}.{DATASET}.dim_chip` T
            USING (SELECT '{id_chip_sql}' AS id_chip) S
            ON T.id_chip = S.id_chip

            WHEN MATCHED THEN UPDATE SET
                numero = '{numero}',
                operadora = '{operadora}',
                operador = '{operador}',
                plano = '{plano}',
                status = '{status}',
                observacao = {observacao_sql},
                dt_inicio = {dt_inicio},
                ultima_recarga_valor = {val_recarga},
                ultima_recarga_data = {dt_recarga},
                total_gasto = {total_gasto},
                sk_aparelho_atual = {aparelho_sql},
                updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                id_chip, numero, operadora, operador, plano, status,
                observacao,
                dt_inicio, ultima_recarga_valor, ultima_recarga_data,
                total_gasto, sk_aparelho_atual,
                ativo, created_at, updated_at
            )
            VALUES (
                '{id_chip_sql}', '{numero}', '{operadora}', '{operador}', '{plano}', '{status}',
                {observacao_sql},
                {dt_inicio}, {val_recarga}, {dt_recarga},
                {total_gasto}, {aparelho_sql},
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """

        self._run(sql)

    # ============================================================
    # MOVIMENTAÇÃO
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
    # EVENTOS
    # ============================================================
    def get_eventos(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.f_chip_aparelho`
            ORDER BY data_movimento DESC
        """
        return self._run(sql)
