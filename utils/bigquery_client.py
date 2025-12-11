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
    """Retorna NULL, valor numérico sem aspas ou string escapada."""
    if value is None or value == "" or str(value).lower() == "none":
        return "NULL"

    value = str(value).strip()

    # tenta interpretar como número
    try:
        float(value.replace(",", "."))
        return value
    except:
        pass

    # string
    value = value.replace("'", "''")
    return f"'{value}'"


# ============================================================
# NORMALIZAÇÃO DE DATA
# ============================================================
def normalize_date(value):
    if not value:
        return "NULL"
    try:
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return f"DATE('{value}')"
        if "/" in value:
            d, m, y = value.split("/")
            return f"DATE('{y}-{m.zfill(2)}-{d.zfill(2)}')"
        if "T" in value:
            return f"DATE('{value.split('T')[0]}')"
        dt = datetime.fromisoformat(value)
        return f"DATE('{dt.strftime('%Y-%m-%d')}')"
    except:
        return "NULL"


# ============================================================
# NORMALIZAÇÃO DE NÚMERO
# ============================================================
def normalize_number(value):
    if not value:
        return "NULL"
    try:
        return str(float(str(value).replace(",", ".")))
    except:
        return "NULL"


# ============================================================
# BIGQUERY CLIENT
# ============================================================
class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT, location=LOCATION)

    # ========================================================
    # EXECUTA SQL
    # ========================================================
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

    # ========================================================
    # GET VIEW
    # ========================================================
    def get_view(self, view_name: str):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.{view_name}`
        """
        return self._run(sql)

    # ========================================================
    # GET APARELHOS
    # ========================================================
    def get_aparelhos(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_aparelhos`
            ORDER BY modelo
        """
        return self._run(sql)

    # ============================================================
    # UPSERT APARELHO
    # ============================================================
    def upsert_aparelho(self, form):

        id_aparelho = q(form.get("id_aparelho"))
        modelo = q(form.get("modelo"))
        marca = q(form.get("marca"))
        imei = q(form.get("imei"))
        status = q(form.get("status") or "ATIVO")

        sql_next = f"""
            SELECT COALESCE(MAX(sk_aparelho),0) + 1 AS next_sk
            FROM `{PROJECT}.{DATASET}.dim_aparelho`
        """
        next_sk = int(self._run(sql_next).iloc[0]["next_sk"])

        sql = f"""
            MERGE `{PROJECT}.{DATASET}.dim_aparelho` T
            USING (SELECT {id_aparelho} AS id_aparelho) S
            ON T.id_aparelho = S.id_aparelho

            WHEN MATCHED THEN UPDATE SET
                modelo = {modelo},
                marca = {marca},
                imei = {imei},
                status = {status},
                updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                sk_aparelho, id_aparelho, modelo, marca, imei, status,
                ativo, created_at, updated_at
            )
            VALUES (
                {next_sk}, {id_aparelho}, {modelo}, {marca}, {imei}, {status},
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """
        self._run(sql)

    # ============================================================
    # UPSERT CHIP
    # ============================================================
    def upsert_chip(self, form):

        sk_chip = form.get("sk_chip")

        # CHIP NOVO → gerar próximo SK
        if not sk_chip:
            sql_next = f"""
                SELECT COALESCE(MAX(sk_chip),0) + 1 AS next_sk
                FROM `{PROJECT}.{DATASET}.dim_chip`
            """
            sk_chip = int(self._run(sql_next).iloc[0]["next_sk"])
            is_new = True
        else:
            sk_chip = int(sk_chip)
            is_new = False

        # — TRATAMENTO CORRETO: numero sempre STRING
        raw_num = form.get("numero")
        if raw_num in [None, "", "None"]:
            numero = "NULL"
        else:
            numero = f"'{str(raw_num).replace(\"'\",\"''\")}'"

        operadora   = q(form.get("operadora"))
        operador    = q(form.get("operador"))
        plano       = q(form.get("plano"))
        status      = q(form.get("status") or "DISPONIVEL")
        observacao  = q(form.get("observacao"))

        dt_inicio   = normalize_date(form.get("dt_inicio"))
        dt_recarga  = normalize_date(form.get("ultima_recarga_data"))
        val_recarga = normalize_number(form.get("ultima_recarga_valor"))
        total_gasto = normalize_number(form.get("total_gasto"))

        # sk_aparelho_atual é INT64
        sk_ap = form.get("sk_aparelho_atual")
        aparelho_sql = sk_ap if sk_ap not in [None, "", "None"] else "NULL"

        # ----------------------------------------------------
        # Se chip já existe → registrar eventos
        # ----------------------------------------------------
        if not is_new:
            sql_old = f"""
                SELECT *
                FROM `{PROJECT}.{DATASET}.dim_chip`
                WHERE sk_chip = {sk_chip}
                LIMIT 1
            """
            antigo_df = self._run(sql_old)
            antigo = antigo_df.to_dict(orient="records")[0] if not antigo_df.empty else None

            if antigo:
                campos = {
                    "numero": numero,
                    "operadora": operadora,
                    "operador": operador,
                    "plano": plano,
                    "status": status,
                    "sk_aparelho_atual": aparelho_sql,
                }

                for campo, novo_sql in campos.items():
                    novo_val = str(novo_sql).replace("'", "") if novo_sql != "NULL" else ""
                    old_val = antigo.get(campo)
                    old_val = "" if old_val in [None, "None", "NULL"] else str(old_val)

                    if old_val != novo_val:
                        self.registrar_evento_chip(
                            sk_chip=sk_chip,
                            tipo_evento=campo.upper(),
                            valor_antigo=old_val,
                            valor_novo=novo_val,
                            origem="Painel",
                            obs="Alteração via painel"
                        )

        # =============================================================
        #   MERGE FINAL (campos com tipos corretos segundo esquema)
        # =============================================================
        sql_merge = f"""
            MERGE `{PROJECT}.{DATASET}.dim_chip` T
            USING (SELECT {sk_chip} AS sk_chip) S
            ON T.sk_chip = S.sk_chip

            WHEN MATCHED THEN UPDATE SET
                numero = {numero},
                operadora = {operadora},
                operador = {operador},
                plano = {plano},
                status = {status},
                observacao = {observacao},
                dt_inicio = {dt_inicio},
                ultima_recarga_valor = {val_recarga},
                ultima_recarga_data = {dt_recarga},
                total_gasto = {total_gasto},
                sk_aparelho_atual = {aparelho_sql},
                updated_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                sk_chip, numero, operadora, operador, plano, status,
                observacao, dt_inicio,
                ultima_recarga_valor, ultima_recarga_data,
                total_gasto, sk_aparelho_atual,
                ativo, created_at, updated_at
            )
            VALUES (
                {sk_chip}, {numero}, {operadora}, {operador}, {plano}, {status},
                {observacao}, {dt_inicio},
                {val_recarga}, {dt_recarga},
                {total_gasto}, {aparelho_sql},
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """
        self._run(sql_merge)

        return sk_chip

    # ============================================================
    # REGISTRAR MOVIMENTO
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
    # REGISTRAR EVENTO
    # ============================================================
    def registrar_evento_chip(self, sk_chip, tipo_evento, valor_antigo, valor_novo, origem, obs):
        query = f"""
            CALL `{PROJECT}.{DATASET}.sp_registrar_evento_chip`(
                @sk, @tipo, @old, @new, @orig, @obs
            )
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("tipo", "STRING", tipo_evento),
                bigquery.ScalarQueryParameter("old", "STRING", valor_antigo),
                bigquery.ScalarQueryParameter("new", "STRING", valor_novo),
                bigquery.ScalarQueryParameter("orig", "STRING", origem),
                bigquery.ScalarQueryParameter("obs", "STRING", obs),
            ]
        )
        self.client.query(query, job_config=job_config).result()
        return True

    # ============================================================
    # TIMELINE
    # ============================================================
    def get_eventos_chip(self, sk_chip):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
            WHERE sk_chip = {sk_chip}
            ORDER BY data_evento DESC
        """
        return self._run(sql)
