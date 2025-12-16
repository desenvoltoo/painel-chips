# -*- coding: utf-8 -*-

import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# ===========================
# VARIÁVEIS DE AMBIENTE
# ===========================
PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# ============================================================
# Sanitização segura
# ============================================================
def q(value):
    """Retorna NULL, número sem aspas ou string escapada."""
    if value in [None, "", "None"]:
        return "NULL"

    value = str(value).strip()

    # número
    try:
        float(value.replace(",", "."))
        return value
    except:
        pass

    # texto
    value = value.replace("'", "''")
    return f"'{value}'"


# ============================================================
# NORMALIZAÇÃO DE DATA
# ============================================================
def normalize_date(value):
    if not value:
        return "NULL"

    value = str(value)

    # yyyy-mm-dd
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return f"DATE('{value}')"

    # dd/mm/yyyy
    if "/" in value:
        try:
            d, m, y = value.split("/")
            return f"DATE('{y}-{m.zfill(2)}-{d.zfill(2)}')"
        except:
            return "NULL"

    # formato ISO
    if "T" in value:
        return f"DATE('{value.split('T')[0]}')"

    return "NULL"


def normalize_number(value):
    if value in [None, "", "None"]:
        return "NULL"
    try:
        return str(float(str(value).replace(",", ".")))
    except:
        return "NULL"


# ============================================================
# CLIENTE BIGQUERY
# ============================================================
class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT, location=LOCATION)
        self.project = PROJECT
        self.dataset = DATASET


    # ============================== EXECUTAR SQL ==============================
    def _run(self, sql: str):
        print("\n🔥 EXECUTANDO SQL:\n", sql, "\n================================")
        job = self.client.query(sql)
        df = job.result().to_dataframe(create_bqstorage_client=False)
        return df.astype(object).where(pd.notnull(df), None)


    # ============================== GET VIEW ==============================
    def get_view(self, view_name):
        return self._run(f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{view_name}`
        """)


    # ============================================================
    # UPSERT DO CHIP (100% CORRIGIDO)
    # ============================================================
    def upsert_chip(self, form: dict):

        # CAPTURA O sk_chip
        sk_chip = form.get("sk_chip")

        if not sk_chip:
            sql_next = f"""
                SELECT COALESCE(MAX(sk_chip),0) + 1 AS next_sk
                FROM `{self.project}.{self.dataset}.dim_chip`
            """
            sk_chip = int(self._run(sql_next).iloc[0]["next_sk"])
            is_new = True
        else:
            sk_chip = int(sk_chip)
            is_new = False

        # CAMPOS
        numero      = q(form.get("numero"))
        operadora   = q(form.get("operadora"))
        operador    = q(form.get("operador"))
        plano       = q(form.get("plano"))
        status      = q(form.get("status"))
        observacao  = q(form.get("observacao"))

        # ✔ campo correto: data_inicio
        data_inicio = normalize_date(form.get("data_inicio"))

        ultima_data   = normalize_date(form.get("ultima_recarga_data"))
        ultima_valor  = normalize_number(form.get("ultima_recarga_valor"))
        total_gasto   = normalize_number(form.get("total_gasto"))

        sk_aparelho = form.get("sk_aparelho")
        sk_aparelho_sql = sk_aparelho if sk_aparelho not in [None, "", "None"] else "NULL"

        # EVENTO DE CRIAÇÃO
        if is_new:
            self.registrar_evento_chip(
                sk_chip, "CRIACAO",
                "", f"Chip {form.get('numero')}",
                "Painel", "Criação via painel"
            )

        # MERGE FINAL
        sql = f"""
            MERGE `{self.project}.{self.dataset}.dim_chip` T
            USING (SELECT {sk_chip} AS sk_chip) S
            ON T.sk_chip = S.sk_chip

            WHEN MATCHED THEN UPDATE SET
                numero = {numero},
                operadora = {operadora},
                operador = {operador},
                plano = {plano},
                status = {status},
                observacao = {observacao},
                data_inicio = {data_inicio},
                ultima_recarga_data = {ultima_data},
                ultima_recarga_valor = {ultima_valor},
                total_gasto = {total_gasto},
                sk_aparelho = {sk_aparelho_sql},
                updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                sk_chip, numero, operadora, operador, plano, status,
                observacao, data_inicio,
                ultima_recarga_data, ultima_recarga_valor,
                total_gasto, sk_aparelho,
                ativo, created_at, updated_at
            )
            VALUES (
                {sk_chip}, {numero}, {operadora}, {operador}, {plano}, {status},
                {observacao}, {data_inicio},
                {ultima_data}, {ultima_valor},
                {total_gasto}, {sk_aparelho_sql},
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """

        self._run(sql)
        return sk_chip


    # ============================================================
    # EVENTOS DO CHIP
    # ============================================================
    def registrar_evento_chip(self, sk_chip, tipo_evento, old, new, origem, obs):

        query = f"""
            CALL `{self.project}.{self.dataset}.sp_registrar_evento_chip`(
                @sk, @tipo, @old, @new, @orig, @obs
            )
        """

        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("sk", "INT64", sk_chip),
            bigquery.ScalarQueryParameter("tipo", "STRING", tipo_evento),
            bigquery.ScalarQueryParameter("old", "STRING", old),
            bigquery.ScalarQueryParameter("new", "STRING", new),
            bigquery.ScalarQueryParameter("orig", "STRING", origem),
            bigquery.ScalarQueryParameter("obs", "STRING", obs),
        ])

        self.client.query(query, job_config=cfg).result()
