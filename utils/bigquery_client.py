from google.cloud import bigquery
import pandas as pd

PROJECT = "painel-universidade"
DATASET = "marts"


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ----------------------------
    # FUNCIONAL GLOBAL
    # ----------------------------
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
    # APARELHOS (DIM)
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
            '{form.get("id_aparelho")}',
            '{form.get("modelo")}',
            '{form.get("marca")}',
            '{form.get("imei")}',
            '{form.get("status")}',
            TRUE
        )
        """
        self.client.query(sql)

    # ----------------------------
    # CHIPS (DIM)
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
            '{form.get("id_chip")}',
            '{form.get("numero")}',
            '{form.get("operadora")}',
            '{form.get("plano")}',
            '{form.get("status")}',
            DATE('{form.get("dt_inicio")}'),
            {form.get("ultima_recarga_valor") or 0},
            DATE('{form.get("ultima_recarga_data")}'),
            {form.get("total_gasto") or 0},
            {form.get("sk_aparelho_atual") or "NULL"},
            TRUE
        )
        """
        self.client.query(sql)

    # ----------------------------
    # EVENTOS (FATO)
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
            {form.get("sk_chip")},
            {form.get("sk_aparelho")},
            DATE('{form.get("data_uso")}'),
            '{form.get("tipo_movimento")}',
            '{form.get("origem")}',
            '{form.get("observacao")}'
        )
        """
        self.client.query(sql)
