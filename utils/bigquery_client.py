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
        self.client = None
        self.project = PROJECT
        self.dataset = DATASET

    def _get_client(self):
        if self.client is None:
            self.client = bigquery.Client(
                project=PROJECT,
                location=LOCATION
            )
        return self.client


    # ========================================================
    # EXECUÇÃO GENÉRICA (SEM DATAFRAME)
    # ========================================================
    def run(self, sql: str):
        print("\n🔥 EXECUTANDO SQL:\n", sql, "\n" + "=" * 80)
        return self._get_client().query(sql).result()


    # ========================================================
    # EXECUÇÃO COM DATAFRAME (LEITURA) — SUPORTE TOTAL A PARAMS
    # ========================================================
    def run_df(self, sql: str, params=None):
        print("\n🔥 EXECUTANDO SQL (DF):\n", sql, "\n" + "=" * 80)

        job_config = None

        # --------------------------------------------
        # params como LISTA de ScalarQueryParameter
        # --------------------------------------------
        if isinstance(params, list):
            job_config = bigquery.QueryJobConfig(
                query_parameters=params
            )

        # --------------------------------------------
        # params como DICT simples
        # --------------------------------------------
        elif isinstance(params, dict):
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        name=k,
                        type_="INT64" if isinstance(v, int) else "STRING",
                        value=v
                    )
                    for k, v in params.items()
                ]
            )

        # --------------------------------------------
        # params None → query simples
        # --------------------------------------------
        elif params is None:
            job_config = None

        else:
            raise TypeError(
                "params deve ser dict, list[ScalarQueryParameter] ou None"
            )

        job = self._get_client().query(sql, job_config=job_config)
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
    """Executa uma Stored Procedure no dataset configurado.

    Args:
        sp_name: nome da SP (sem projeto/dataset), ex.: "sp_upsert_chip".
        params: string já formatada com os parâmetros, ex.:
            "'ID123','11999999999','VIVO','PRE','ATIVO'"

    Exemplo:
        bq.call_sp(
            "sp_upsert_chip",
            "123,'11999999999','VIVO','PRE','Obs','Operador'"
        )
    """
    sql = f"CALL {self.project}.{self.dataset}.{sp_name}({params})"
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
