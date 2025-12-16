# -*- coding: utf-8 -*-

import os
import pandas as pd
from google.cloud import bigquery

# ============================================================
# VARIÁVEIS DE AMBIENTE
# ============================================================
PROJECT  = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET  = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# ============================================================
# HELPERS
# ============================================================
def q(value):
    """STRING segura para SQL"""
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

    # ISO
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


    # ========================================================
    # EXECUTAR SQL
    # ========================================================
    def _run(self, sql: str):
        print("\n🔥 EXECUTANDO SQL:\n", sql, "\n" + "="*50)
        job = self.client.query(sql)
        df = job.result().to_dataframe(create_bqstorage_client=False)
        return df.astype(object).where(pd.notnull(df), None)


    # ========================================================
    # GET VIEW
    # ========================================================
    def get_view(self, view_name: str):
        return self._run(f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{view_name}`
        """)


    # ========================================================
    # UPSERT CHIP — DEFINITIVO
    # ========================================================
    def upsert_chip(self, form: dict):

        # ----------------------------------------------------
        # IDENTIFICA CHIP
        # ----------------------------------------------------
        sk_chip = form.get("sk_chip")

        if sk_chip in [None, "", "None"]:
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
                    dt_inicio,                     -- 🔧 nome físico
                    ultima_recarga_data,
                    ultima_recarga_valor,
                    total_gasto,
                    sk_aparelho
                FROM `{self.project}.{self.dataset}.dim_chip`
                WHERE sk_chip = {sk_chip}
                LIMIT 1
            """)

            antigo = antigo_df.iloc[0].to_dict() if not antigo_df.empty else None


        # ----------------------------------------------------
        # NOVOS VALORES (PARA HISTÓRICO)
        # ----------------------------------------------------
        novos = {
            "numero": str(form.get("numero") or ""),
            "operadora": str(form.get("operadora") or ""),
            "operador": str(form.get("operador") or ""),
            "plano": str(form.get("plano") or ""),
            "status": str(form.get("status") or ""),
            "dt_inicio": str(form.get("dt_inicio") or ""),   # 🔧
            "sk_aparelho": str(form.get("sk_aparelho") or ""),
        }


        # ----------------------------------------------------
        # EVENTO CRIAÇÃO
        # ----------------------------------------------------
        if is_new:
            self.registrar_evento_chip(
                sk_chip,
                "CRIACAO",
                "",
                f"Chip {novos['numero']}",
                "Painel",
                "Criação via painel"
            )


        # ----------------------------------------------------
        # EVENTOS ALTERAÇÃO
        # ----------------------------------------------------
        if antigo:
            for campo, novo_val in novos.items():
                old_val = antigo.get(campo)
                old_val = "" if old_val is None else str(old_val)

                if old_val != novo_val:
                    self.registrar_evento_chip(
                        sk_chip,
                        campo.upper(),
                        old_val,
                        novo_val,
                        "Painel",
                        "Alteração via painel"
                    )


        # ----------------------------------------------------
        # NORMALIZAÇÃO SQL
        # ----------------------------------------------------
        numero      = q(form.get("numero"))
        operadora   = q(form.get("operadora"))
        operador    = q(form.get("operador"))
        plano       = q(form.get("plano"))
        status      = q(form.get("status"))
        observacao  = q(form.get("observacao"))

        dt_inicio   = normalize_date(form.get("dt_inicio"))  # 🔧
        rec_data    = normalize_date(form.get("ultima_recarga_data"))
        rec_valor   = normalize_number(form.get("ultima_recarga_valor"))
        total_gasto = normalize_number(form.get("total_gasto"))

        sk_aparelho = form.get("sk_aparelho")
        sk_ap_sql   = sk_aparelho if sk_aparelho not in [None, "", "None"] else "NULL"


        # ----------------------------------------------------
        # MERGE FINAL
        # ----------------------------------------------------
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
            dt_inicio = {dt_inicio},                 -- 🔧
            ultima_recarga_data = {rec_data},
            ultima_recarga_valor = {rec_valor},
            total_gasto = {total_gasto},
            sk_aparelho = {sk_ap_sql},
            updated_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT (
            sk_chip, numero, operadora, operador, plano, status,
            observacao, dt_inicio,                   -- 🔧
            ultima_recarga_data, ultima_recarga_valor,
            total_gasto, sk_aparelho,
            ativo, created_at, updated_at
        )
        VALUES (
            {sk_chip}, {numero}, {operadora}, {operador}, {plano}, {status},
            {observacao}, {dt_inicio},
            {rec_data}, {rec_valor},
            {total_gasto}, {sk_ap_sql},
            TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """

        self._run(sql)
        return sk_chip


    # ========================================================
    # REGISTRAR EVENTO CHIP (SP)
    # ========================================================
    def registrar_evento_chip(self, sk_chip, tipo_evento, old, new, origem, obs):

        query = f"""
            CALL `{self.project}.{self.dataset}.sp_registrar_evento_chip`(
                @sk, @tipo, @old, @new, @orig, @obs
            )
        """

        cfg = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("tipo", "STRING", tipo_evento),
                bigquery.ScalarQueryParameter("old", "STRING", old),
                bigquery.ScalarQueryParameter("new", "STRING", new),
                bigquery.ScalarQueryParameter("orig", "STRING", origem),
                bigquery.ScalarQueryParameter("obs", "STRING", obs),
            ]
        )

        self.client.query(query, job_config=cfg).result()
        return True
