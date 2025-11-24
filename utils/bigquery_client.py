from google.cloud import bigquery
import pandas as pd
from datetime import datetime

PROJECT = "painel-universidade"
DATASET = "marts"


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ============================================================
    # EXECUÇÃO DE SQL
    # ============================================================
    def _run(self, sql: str):
        try:
            return self.client.query(sql).to_dataframe()
        except Exception as e:
            print("\n🚨 ERRO NO SQL 🚨")
            print(sql)
            print(e)
            raise e

    # ============================================================
    # VIEW PRINCIPAL
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
        id_aparelho = form.get("id_aparelho")
        modelo = form.get("modelo")
        marca = form.get("marca")
        imei = form.get("imei")
        status = form.get("status") or "ATIVO"

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
          INSERT (id_aparelho, modelo, marca, imei, status, ativo, create_at, update_at)
          VALUES ('{id_aparelho}', '{modelo}', '{marca}', '{imei}', '{status}', TRUE,
                  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        """

        self._run(sql)

    # ============================================================
    # ========================= CHIPS =============================
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
        id_chip = form.get("id_chip")
        numero = form.get("numero")
        operadora = form.get("operadora")
        plano = form.get("plano")
        status = form.get("status")

        dt_inicio = form.get("dt_inicio") or None
        ultima_data = form.get("ultima_recarga_data") or None
        val_recarga = form.get("ultima_recarga_valor") or "0"
        total_gasto = form.get("total_gasto") or "0"

        sk_aparelho_atual = form.get("sk_aparelho_atual")
        sk_aparelho_atual = sk_aparelho_atual if sk_aparelho_atual not in ["", None] else None

        sql = f"""
        MERGE `{PROJECT}.{DATASET}.dim_chip` T
        USING (SELECT '{id_chip}' AS id_chip) S
        ON T.id_chip = S.id_chip

        WHEN MATCHED THEN
          UPDATE SET
            numero = '{numero}',
            operadora = '{operadora}',
            plano = '{plano}',
            status = '{status}',
            dt_inicio = {f"DATE('{dt_inicio}')" if dt_inicio else "NULL"},
            ultima_recarga_valor = {val_recarga},
            ultima_recarga_data = {f"DATE('{ultima_data}')" if ultima_data else "NULL"},
            total_gasto = {total_gasto},
            sk_aparelho_atual = {sk_aparelho_atual if sk_aparelho_atual else "NULL"},
            update_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
          INSERT (
            id_chip, numero, operadora, plano, status,
            dt_inicio, ultima_recarga_valor, ultima_recarga_data,
            total_gasto, sk_aparelho_atual,
            ativo, create_at, update_at
          )
          VALUES (
            '{id_chip}', '{numero}', '{operadora}', '{plano}', '{status}',
            {f"DATE('{dt_inicio}')" if dt_inicio else "NULL"},
            {val_recarga},
            {f"DATE('{ultima_data}')" if ultima_data else "NULL"},
            {total_gasto},
            {sk_aparelho_atual if sk_aparelho_atual else "NULL"},
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
          );
        """

        self._run(sql)

    # ============================================================
    # ================== FATO DE MOVIMENTAÇÃO =====================
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
        tipo = form.get("tipo_movimento")
        origem = form.get("origem") or None
        obs = form.get("observacao") or None

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
