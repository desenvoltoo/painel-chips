from google.cloud import bigquery
import pandas as pd
from datetime import datetime

PROJECT = "painel-universidade"
DATASET = "marts"


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ----------------------------
    # Execução de SQL segura
    # ----------------------------
    def _run(self, sql: str):
        try:
            return self.client.query(sql).to_dataframe()
        except Exception as e:
            print("\n🚨 ERRO NO SQL 🚨")
            print(sql)
            print(e)
            raise e

    # ----------------------------
    # VIEW PRINCIPAL
    # ----------------------------
    def get_view(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY sk_chip
        """
        return self._run(sql)

    # ----------------------------
    # DIM APARELHO
    # ----------------------------
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

    def insert_aparelho(self, form):
        id_aparelho = form.get("id_aparelho") or None
        modelo = form.get("modelo") or None
        marca = form.get("marca") or None
        imei = form.get("imei") or None
        status = form.get("status") or "ATIVO"

        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_aparelho`
        (id_aparelho, modelo, marca, imei, status, ativo, create_at, update_at)
        VALUES (
            {'NULL' if id_aparelho is None else f"'{id_aparelho}'"},
            {'NULL' if modelo is None else f"'{modelo}'"},
            {'NULL' if marca is None else f"'{marca}'"},
            {'NULL' if imei is None else f"'{imei}'"},
            '{status}',
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """
        self._run(sql)

    # ----------------------------
    # DIM CHIP
    # ----------------------------
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

    def insert_chip(self, form):
        # Campos string
        id_chip = form.get("id_chip") or None
        numero = form.get("numero") or None
        operadora = form.get("operadora") or None
        plano = form.get("plano") or None
        status = form.get("status") or "DISPONIVEL"

        # Datas (date-safe)
        dt_inicio = form.get("dt_inicio") or None
        ultima_data = form.get("ultima_recarga_data") or None

        # Numerics (numeric-safe)
        val_recarga = form.get("ultima_recarga_valor") or "0"
        total_gasto = form.get("total_gasto") or "0"

        # FK Aparelho
        aparelho = form.get("sk_aparelho_atual")
        aparelho = aparelho if aparelho not in ["", None] else "NULL"

        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_chip`
        (
            id_chip, numero, operadora, plano, status, dt_inicio,
            ultima_recarga_valor, ultima_recarga_data,
            total_gasto, sk_aparelho_atual,
            ativo, create_at, update_at
        )
        VALUES(
            {'NULL' if id_chip is None else f"'{id_chip}'"},
            {'NULL' if numero is None else f"'{numero}'"},
            {'NULL' if operadora is None else f"'{operadora}'"},
            {'NULL' if plano is None else f"'{plano}'"},
            '{status}',
            {f"DATE('{dt_inicio}')" if dt_inicio else "NULL"},
            {val_recarga},
            {f"DATE('{ultima_data}')" if ultima_data else "NULL"},
            {total_gasto},
            {aparelho},
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
        """
        self._run(sql)

    # ----------------------------
    # FATO f_chip_aparelho
    # ----------------------------
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
        )
        """
        self._run(sql)
