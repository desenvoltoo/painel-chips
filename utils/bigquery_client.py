from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import uuid

PROJECT = "painel-universidade"
DATASET = "marts"


def q(x: str):
    """Escapa aspas simples."""
    if x is None:
        return None
    return x.replace("'", "''")


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ============================================================
    # EXECUÇÃO DE SQL
    # ============================================================
    def _run(self, sql: str, job_config=None):
        try:
            job = self.client.query(sql, job_config=job_config)
            return job.to_dataframe()
        except Exception as e:
            print("\n🚨 ERRO NO SQL 🚨")
            print(sql)
            print(e)
            raise e

    # ============================================================
    # VIEW PRINCIPAL — DASHBOARD
    # ============================================================
    def get_view(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY numero
        """
        return self._run(sql)

    # ============================================================
    # ======================= APARELHOS ===========================
    # ============================================================

    def get_view(self):
    sql = """
        SELECT 
            id_chip,
            numero,
            operadora,
            plano,
            status,
            ultima_recarga_data,
            modelo_aparelho
        FROM `painel-universidade.marts.vw_chips_dashboard`
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
            FROM `{PROJECT}.{DATASET}.dim_aparelho`;
        """
        df = self._run(sql_next)
        next_sk = int(df.iloc[0]["next_sk"])

        sql = f"""
        MERGE `{PROJECT}.{DATASET}.dim_aparelho` T
        USING (SELECT '{id_aparelho}' AS id_aparelho) S
        ON T.id_aparelho = S.id_aparelho

        WHEN MATCHED THEN
          UPDATE SET
            modelo = '{modelo}',
            marca = '{marca}',
            imei = '{imei}',
            status = '{status}',
            updated_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
          INSERT (
            sk_aparelho,
            id_aparelho,
            modelo,
            marca,
            imei,
            status,
            ativo,
            created_at,
            updated_at
          )
          VALUES (
            {next_sk},
            '{id_aparelho}',
            '{modelo}',
            '{marca}',
            '{imei}',
            '{status}',
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          );
        """
        self._run(sql)

    # ============================================================
    # =========================== CHIPS ===========================
    # ============================================================

    def get_chips(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY numero
        """
        return self._run(sql)

    def upsert_chip(self, form):

        id_chip = form.get("id_chip")
        if not id_chip or id_chip.strip() == "":
            id_chip = str(uuid.uuid4())
        id_chip_sql = q(id_chip)

        numero = q(form.get("numero"))
        operadora = q(form.get("operadora"))
        plano = q(form.get("plano"))
        status = q(form.get("status"))

        # datas
        def sql_date(x):
            return f"DATE('{x}')" if x else "NULL"

        dt_inicio_sql = sql_date(form.get("dt_inicio"))
        ultima_recarga_sql = sql_date(form.get("ultima_recarga_data"))

        # números
        def sql_num(x):
            try:
                if x is None or x == "":
                    return 0
                x = x.replace(",", ".")
                return float(x)
            except:
                return 0

        val_recarga = sql_num(form.get("ultima_recarga_valor"))
        total_gasto = sql_num(form.get("total_gasto"))

        sk_aparelho_atual = form.get("sk_aparelho_atual")
        aparelho_sql = sk_aparelho_atual if sk_aparelho_atual else "NULL"

        sql = f"""
        MERGE `{PROJECT}.{DATASET}.dim_chip` T
        USING (SELECT '{id_chip_sql}' AS id_chip) S
        ON T.id_chip = S.id_chip

        WHEN MATCHED THEN UPDATE SET
            numero = '{numero}',
            operadora = '{operadora}',
            plano = '{plano}',
            status = '{status}',
            dt_inicio = {dt_inicio_sql},
            ultima_recarga_valor = {val_recarga},
            ultima_recarga_data = {ultima_recarga_sql},
            total_gasto = {total_gasto},
            sk_aparelho_atual = {aparelho_sql},
            updated_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT (
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
            created_at,
            updated_at
        )
        VALUES (
            '{id_chip_sql}',
            '{numero}',
            '{operadora}',
            '{plano}',
            '{status}',
            {dt_inicio_sql},
            {val_recarga},
            {ultima_recarga_sql},
            {total_gasto},
            {aparelho_sql},
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        );
        """

        self._run(sql)

    # ============================================================
    # ======================= MOVIMENTAÇÃO ========================
    # ============================================================

    def registrar_movimento_chip(self, sk_chip, sk_aparelho, tipo, origem, observacao):
        """
        Chama a stored procedure oficial de movimentação.
        """
        QUERY = f"""
        CALL `{PROJECT}.{DATASET}.sp_registrar_movimento_chip`(
            @sk_chip,
            @sk_aparelho,
            @tipo,
            @origem,
            @observacao
        );
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", sk_aparelho),
                bigquery.ScalarQueryParameter("tipo", "STRING", tipo),
                bigquery.ScalarQueryParameter("origem", "STRING", origem),
                bigquery.ScalarQueryParameter("observacao", "STRING", observacao),
            ]
        )

        self.client.query(QUERY, job_config=job_config).result()
        return True

    def get_eventos(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.f_chip_aparelho`
        ORDER BY data_movimento DESC
        """
        return self._run(sql)
