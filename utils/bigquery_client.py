# utils/bigquery_client.py
from google.cloud import bigquery
import pandas as pd

PROJECT = "painel-universidade"
DATASET = "marts"

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ----------------------------
    # HELPERS SEGUROS
    # ----------------------------
    def _q(self, v):
        if not v or v == "":
            return "NULL"
        return f"'{v}'"

    def _num(self, v):
        if not v or v == "":
            return "0"
        return v

    def _num_or_null(self, v):
        if not v or v == "":
            return "NULL"
        return v

    def _run(self, sql: str):
        try:
            return self.client.query(sql).to_dataframe()
        except Exception as e:
            print("ERRO SQL:", sql)
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
    # APARELHOS
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
        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_aparelho`
        (id_aparelho, modelo, marca, imei, status, ativo)
        VALUES(
            {self._q(form.get("id_aparelho"))},
            {self._q(form.get("modelo"))},
            {self._q(form.get("marca"))},
            {self._q(form.get("imei"))},
            {self._q(form.get("status"))},
            TRUE
        )
        """
        self.client.query(sql)

    # ----------------------------
    # CHIPS
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
        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_chip`
        (
            id_chip, numero, operadora, plano, status, dt_inicio,
            ultima_recarga_valor, ultima_recarga_data, total_gasto,
            sk_aparelho_atual, ativo
        )
        VALUES(
            {self._q(form.get("id_chip"))},
            {self._q(form.get("numero"))},
            {self._q(form.get("operadora"))},
            {self._q(form.get("plano"))},
            {self._q(form.get("status"))},
            DATE({self._q(form.get("dt_inicio"))}),
            {self._num(form.get("ultima_recarga_valor"))},
            DATE({self._q(form.get("ultima_recarga_data"))}),
            {self._num(form.get("total_gasto"))},
            {self._num_or_null(form.get("sk_aparelho_atual"))},
            TRUE
        )
        """
        self.client.query(sql)

    # ----------------------------
    # MOVIMENTAÇÕES (FATO)
    # ----------------------------
    def get_eventos(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.f_chip_aparelho`
        ORDER BY data_uso DESC
        """
        return self._run(sql)

    def insert_evento(self, form):
        sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.f_chip_aparelho`
        (sk_chip, sk_aparelho, data_uso, tipo_movimento, origem, observacao)
        VALUES(
            {self._num(form.get("sk_chip"))},
            {self._num(form.get("sk_aparelho"))},
            DATE({self._q(form.get("data_uso"))}),
            {self._q(form.get("tipo_movimento"))},
            {self._q(form.get("origem"))},
            {self._q(form.get("observacao"))}
        )
        """
        self.client.query(sql)
