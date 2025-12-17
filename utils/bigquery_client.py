# -*- coding: utf-8 -*-

import os
import pandas as pd
from google.cloud import bigquery

PROJECT  = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET  = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# ============================================================
# BIGQUERY CLIENT — LEITURA + EXECUÇÃO DE SPs
# ============================================================
class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(
            project=PROJECT,
            location=LOCATION
        )
        self.project = PROJECT
        self.dataset = DATASET


    # ========================================================
    # EXECUÇÃO GENÉRICA (SEM DATAFRAME)
    # ========================================================
    def run(self, sql: str):
        print("\n🔥 EXECUTANDO SQL:\n", sql, "\n" + "=" * 80)
        return self.client.query(sql).result()


    # ========================================================
    # EXECUÇÃO COM DATAFRAME (LEITURA)
    # ========================================================
    def run_df(self, sql: str):
        print("\n🔥 EXECUTANDO SQL (DF):\n", sql, "\n" + "=" * 80)
        job = self.client.query(sql)
        df = job.result().to_dataframe(create_bqstorage_client=False)
        return df.astype(object).where(pd.notnull(df), None)


    # ========================================================
    # LEITURA DE VIEWS (PADRÃO DO PAINEL)
    # ========================================================
    def get_view(self, view_name: str):
        return self.run_df(f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{view_name}`
        """)


    # ========================================================
    # 🔧 EXECUTAR STORED PROCEDURE (UTIL DO PAINEL)
    # ========================================================
    def call_sp(self, sp_name: str, params: str):
        """
        Exemplo:
        call_sp(
            "sp_upsert_chip",
            "'ID123','11999999999','VIVO','PRE','ATIVO'"
        )
        """
        sql = f"""
        CALL `{self.project}.{self.dataset}.{sp_name}`(
            {params}
        )
        """
        return self.run(sql)


    # ========================================================
    # ⚠️ BLOQUEIO EXPLÍCITO — PROTEÇÃO DE ARQUITETURA
    # ========================================================
    def upsert_chip(self, *args, **kwargs):
        """
        ❌ NÃO UTILIZAR ESTE MÉTODO NO PAINEL.

        O Painel de Chips deve utilizar EXCLUSIVAMENTE
        Stored Procedures para garantir:
          - histórico correto
          - vínculo consistente
          - auditoria completa
          - arquitetura desacoplada

        Use:
          - sp_upsert_chip
          - sp_alterar_status_chip
          - sp_registrar_recarga_chip
          - sp_vincular_aparelho_chip
          - sp_registrar_movimento_chip
        """
        raise RuntimeError(
            "upsert_chip() BLOQUEADO. "
            "Utilize exclusivamente Stored Procedures."
        )
