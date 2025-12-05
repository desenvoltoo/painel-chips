# -*- coding: utf-8 -*-

import os
import uuid
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# ===========================
# VARIÁVEIS DE AMBIENTE
# ===========================
PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# Sanitizador simples para strings SQL
def q(value: str):
    if value in (None, "", "None"):
        return ""
    return str(value).replace("'", "''")


# ------------------------------
# 🔧 Normalizar datas
# ------------------------------
def normalize_date(value):
    """
    Converte datas vindas do frontend para formato padrão BigQuery → DATE('YYYY-MM-DD').
    """
    if not value:
        return "NULL"

    try:
        # Se já vier no formato certo
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return f"DATE('{value}')"

        # DD/MM/YYYY → YYYY-MM-DD
        if "/" in value:
            d, m, y = value.split("/")
            return f"DATE('{y}-{m.zfill(2)}-{d.zfill(2)}')"

        # Timestamps → pega só a data
        if "T" in value:
            date_part = value.split("T")[0]
            return f"DATE('{date_part}')"

        # Tentativa genérica
        dt = datetime.fromisoformat(value)
        return f"DATE('{dt.strftime('%Y-%m-%d')}')"

    except:
        return "NULL"


# ------------------------------
# 🔧 Normalizar números
# ------------------------------
def normalize_number(value):
    if not value:
        return "NULL"

    try:
        value = str(value).replace(",", ".")
        return str(float(value))
    except:
        return "NULL"


class BigQueryClient:

    def __init__(self):
        self.project = PROJECT
        self.dataset = DATASET
        self.client = bigquery.Client(project=PROJECT, location=LOCATION)

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
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_aparelhos`
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
            SELECT COALESCE(MAX(sk_aparelho),0) + 1 AS next_sk
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
                sk_aparelho, id_aparelho, modelo, marca, imei, status,
                ativo, created_at, updated_at
            )
            VALUES (
                {next_sk}, '{id_aparelho}', '{modelo}', '{marca}', '{imei}', '{status}',
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """

        self._run(sql)

    # ============================================================
    # 📡 CHIPS — UPSERT COMPLETO + EVENTOS
    # ============================================================
    def upsert_chip(self, form):

        # 1) Identificação do chip
        id_chip = form.get("id_chip") or str(uuid.uuid4())
        id_chip_sql = q(id_chip)

        # 2) Buscar estado anterior
        sql_busca = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE id_chip = '{id_chip_sql}'
            LIMIT 1
        """

        atual_df = self._run(sql_busca)
        antigo = atual_df.to_dict(orient="records")[0] if not atual_df.empty else None

        # 3) Coleta de valores novos
        numero = q(form.get("numero"))
        operadora = q(form.get("operadora"))
        operador = q(form.get("operador"))
        plano = q(form.get("plano"))
        status = q(form.get("status") or "DISPONIVEL")
        observacao = q(form.get("observacao") or "")

        dt_inicio = normalize_date(form.get("dt_inicio"))
        dt_recarga = normalize_date(form.get("ultima_recarga_data"))

        val_recarga = normalize_number(form.get("ultima_recarga_valor"))
        total_gasto = normalize_number(form.get("total_gasto"))

        sk_ap = form.get("sk_aparelho_atual")
        aparelho_sql = sk_ap if sk_ap else "NULL"

        # 4) Detectar eventos
        if antigo:
            campos = {
                "numero": numero,
                "operadora": operadora,
                "operador": operador,
                "plano": plano,
                "status": status,
                "sk_aparelho_atual": sk_ap,
            }

            for campo, novo_valor in campos.items():
                old = str(antigo.get(campo) or "")
                new = str(novo_valor or "")

                if old != new:
                    self.registrar_evento_chip(
                        sk_chip=antigo["sk_chip"],
                        tipo_evento=campo.upper(),
                        valor_antigo=old,
                        valor_novo=new,
                        origem="Painel",
                        obs="Alteração via painel"
                    )

        # 5) Merge final
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
                observacao = '{observacao}',
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
                '{observacao}',
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
    def registrar_evento_chip(self, sk_chip, tipo_evento, valor_antigo, valor_novo, origem, obs):

        query = f"""
            CALL `{PROJECT}.{DATASET}.sp_registrar_evento_chip`(
                @sk_chip, @tipo_evento, @valor_old, @valor_new, @origem, @obs
            )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("tipo_evento", "STRING", tipo_evento),
                bigquery.ScalarQueryParameter("valor_old", "STRING", valor_antigo),
                bigquery.ScalarQueryParameter("valor_new", "STRING", valor_novo),
                bigquery.ScalarQueryParameter("origem", "STRING", origem),
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
