from google.cloud import bigquery
import pandas as pd
import datetime

PROJECT = "painel-universidade"
DATASET = "marts"

TABLE_CHIP = f"{PROJECT}.{DATASET}.dim_chip"
TABLE_APARELHO = f"{PROJECT}.{DATASET}.dim_aparelho"
TABLE_EVENTOS = f"{PROJECT}.{DATASET}.f_chip_aparelho"
VIEW_PAINEL = f"{PROJECT}.{DATASET}.vw_chips_painel"


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ---------------------------
    # CONSULTA GENERICA
    # ---------------------------
    def _run(self, sql):
        try:
            return self.client.query(sql).to_dataframe()
        except Exception as e:
            print("ERRO SQL:", e)
            raise e

    # ---------------------------
    # VIEW PRINCIPAL
    # ---------------------------
    def get_view(self):
        return self._run(f"SELECT * FROM `{VIEW_PAINEL}` ORDER BY sk_chip DESC")

    # ---------------------------
    # CHIPS
    # ---------------------------
    def get_chips(self):
        return self._run(f"SELECT * FROM `{TABLE_CHIP}` ORDER BY sk_chip DESC")

    def insert_chip(self, data: dict):

        def to_int(v):
            try:
                return int(v) if v not in ["", None] else None
            except:
                return None

        def to_float(v):
            try:
                return float(v) if v not in ["", None] else None
            except:
                return None

        row = [{
            "id_chip": data.get("id_chip"),
            "numero": data.get("numero"),
            "operadora": data.get("operadora"),
            "plano": data.get("plano"),
            "status": data.get("status"),
            "dt_inicio": data.get("dt_inicio") or None,
            "ultima_recarga_valor": to_float(data.get("ultima_recarga_valor")),
            "ultima_recarga_data": data.get("ultima_recarga_data") or None,
            "total_gasto": to_float(data.get("total_gasto")),
            "sk_aparelho_atual": to_int(data.get("sk_aparelho_atual")),
            "ativo": True,
            "create_at": datetime.datetime.utcnow(),
            "update_at": datetime.datetime.utcnow()
        }]

        errors = self.client.insert_rows_json(TABLE_CHIP, row)
        if errors:
            raise Exception(errors)
        return True

    # ---------------------------
    # APARELHOS
    # ---------------------------
    def get_aparelhos(self):
        return self._run(f"SELECT * FROM `{TABLE_APARELHO}` ORDER BY modelo")

    def insert_aparelho(self, data: dict):

        row = [{
            "id_aparelho": data.get("id_aparelho"),
            "modelo": data.get("modelo"),
            "marca": data.get("marca"),
            "imei": data.get("imei"),
            "status": data.get("status"),
            "ativo": True,
            "create_at": datetime.datetime.utcnow(),
            "update_at": datetime.datetime.utcnow()
        }]

        errors = self.client.insert_rows_json(TABLE_APARELHO, row)
        if errors:
            raise Exception(errors)
        return True

    # ---------------------------
    # EVENTOS
    # ---------------------------
    def get_eventos(self):
        return self._run(f"SELECT * FROM `{TABLE_EVENTOS}` ORDER BY data_uso DESC")

    def insert_evento(self, data: dict):

        row = [{
            "sk_chip": data.get("sk_chip"),
            "sk_aparelho": data.get("sk_aparelho"),
            "data_uso": data.get("data_uso") or None,
            "tipo_movimento": data.get("tipo_movimento"),
            "origem": data.get("origem"),
            "observacao": data.get("observacao"),
            "create_at": datetime.datetime.utcnow(),
            "update_at": datetime.datetime.utcnow()
        }]

        errors = self.client.insert_rows_json(TABLE_EVENTOS, row)
        if errors:
            raise Exception(errors)
        return True
