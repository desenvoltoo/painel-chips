# -*- coding: utf-8 -*-

import os
import pandas as pd
from google.cloud import bigquery

PROJECT  = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET  = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# ============================================================
# HELPERS
# ============================================================
def q(value):
    if value in [None, "", "None"]:
        return "NULL"
    value = str(value).strip().replace("'", "''")
    return f"'{value}'"


def normalize_number(value):
    if value in [None, "", "None"]:
        return "NULL"
    try:
        return str(float(str(value).replace(",", ".")))
    except:
        return "NULL"


def normalize_date(value):
    if value in [None, "", "None"]:
        return "NULL"

    value = str(value)

    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return f"DATE('{value}')"

    if "/" in value:
        try:
            d, m, y = value.split("/")
            return f"DATE('{y}-{m.zfill(2)}-{d.zfill(2)}')"
        except:
            return "NULL"

    if "T" in value:
        return f"DATE('{value.split('T')[0]}')"

    return "NULL"


# ============================================================
# BIGQUERY CLIENT
# ============================================================
class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT, location=LOCATION)
        self.project = PROJECT
        self.dataset = DATASET


    def _run(self, sql: str):
        print("\n🔥 EXECUTANDO SQL:\n", sql, "\n" + "=" * 50)
        job = self.client.query(sql)
        df = job.result().to_dataframe(create_bqstorage_client=False)
        return df.astype(object).where(pd.notnull(df), None)


    def get_view(self, view_name: str):
        return self._run(f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{view_name}`
        """)


    # ========================================================
    # UPSERT CHIP — ALINHADO AO SCHEMA REAL
    # ========================================================
    def upsert_chip(self, form: dict):

        # ---------- IDENTIFICA CHIP ----------
        sk_chip = form.get("sk_chip")

        if not sk_chip:
            sk_chip = int(self._run(f"""
                SELECT COALESCE(MAX(sk_chip),0) + 1 AS sk
                FROM `{self.project}.{self.dataset}.dim_chip`
            """).iloc[0]["sk"])
            is_new = True
            antigo = None
        else:
            sk_chip = int(sk_chip)
            is_new = False

            antigo_df = self._run(f"""
                SELECT
                    numero,
                    operadora,
                    operador,
                    plano,
                    status,
                    dt_inicio,
                    ultima_recarga_data,
                    ultima_recarga_valor,
                    total_gasto,
                    sk_aparelho_atual
                FROM `{self.project}.{self.dataset}.dim_chip`
                WHERE sk_chip = {sk_chip}
                LIMIT 1
            """)

            antigo = antigo_df.iloc[0].to_dict() if not antigo_df.empty else None


        # ---------- NORMALIZA DATA (ACEITA data_inicio OU dt_inicio)
