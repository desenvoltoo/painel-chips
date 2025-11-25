from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import uuid

PROJECT = "painel-universidade"
DATASET = "marts"


def q(x: str):
    """Escapa aspas simples para evitar quebra de SQL."""
    if x is None:
        return None
    return x.replace("'", "''")


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ============================================================
    # EXECUÇÃO DE SQL
    # ============================================================
    def _run(self, sql: str):
        try:
            job = self.client.query(sql)
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
        ORDER BY sk_chip
        """
        return self._run(sql)

    # ============================================================
    # ======================= APARELHOS ===========================
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

        sql_next_sk = f"""
            SELECT COALESCE(MAX(sk_aparelho), 0) + 1 AS next_sk
            FROM `{PROJECT}.{DATASET}.dim_aparelho`;
        """
        df = self._run(sql_next_sk)
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
            update_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
          INSERT (
            sk_aparelho,
            id_aparelho,
            modelo,
            marca,
            imei,
            status,
            ativo,
            create_at,
            update_at
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
        SELECT 
            sk_chip,
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
            ativo
        FROM `{PROJECT}.{DATASET}.dim_chip`
        ORDER BY numero
        """
        return self._run(sql)

    def upsert_chip(self, form):

        # --- ID CHIP ---
        id_chip = form.get("id_chip")
        if not id_chip or id_chip.strip() == "":
            id_chip = str(uuid.uuid4())

        id_chip_sql = q(id_chip)

        # --- STRING FIELDS ---
        numero = q(form.get("numero", "").strip())
        operadora = q(form.get("operadora", "").strip())
        plano = q(form.get("plano", "").strip())
        status = q(form.get("status", "").strip())

        # --- DATAS ---
        def sql_date(x):
            return f"DATE('{x}')" if x else "NULL"

        dt_inicio_sql = sql_date(form.get("dt_inicio"))
        ultima_data_sql = sql_date(form.get("ultima_recarga_data"))

        # --- NÚMEROS (corrigido!) ---
        def sql_num(x):
            try:
                if x is None:
                    return 0
                x = str(x).replace(",", ".").strip()
                return float(x)
            except:
                return 0

        val_recarga = sql_num(form.get("ultima_recarga_valor"))
        total_gasto = sql_num(form.get("total_gasto"))

        # --- APARELHO ---
        aparelho = form.get("sk_aparelho_atual")
        aparelho_sql = aparelho if aparelho not in (None, "") else "NULL"

        # --- MERGE ---
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
            ultima_recarga_data = {ultima_data_sql},
            total_gasto = {total_gasto},
            sk_aparelho_atual = {aparelho_sql},
            update_at = CURRENT_TIMESTAMP()

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
            create_at,
            update_at
        )
        VALUES (
            '{id_chip_sql}',
            '{numero}',
            '{operadora}',
            '{plano}',
            '{status}',
            {dt_inicio_sql},
            {val_recarga},
            {ultima_data_sql},
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

    def get_eventos(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.f_chip_aparelho`
        ORDER BY data_uso DESC
        """
        return self._run(sql)

    def insert_evento(self, form):
        sk_chip = form.get("sk_chip")
        sk_aparelho = form.get("sk_aparelho")
        data_uso = form.get("data_uso")
        tipo = q(form.get("tipo_movimento"))
        origem = q(form.get("origem"))
        obs = q(form.get("observacao"))

        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.f_chip_aparelho`
        (sk_chip, sk_aparelho, data_uso, tipo_movimento, origem, observacao, create_at, update_at)
        VALUES(
            {sk_chip},
            {sk_aparelho},
            DATE('{data_uso}'),
            '{tipo}',
            {'NULL' if origem is None else f"'{origem}'"},
            {'NULL' if obs is None else f"'{obs}'"},
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        );
        """

        self._run(sql)
