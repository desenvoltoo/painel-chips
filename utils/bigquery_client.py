from google.cloud import bigquery
import pandas as pd
import datetime


PROJECT = "painel-universidade"
DATASET = "marts"

TABLE_CHIP = f"{PROJECT}.{DATASET}.dim_chip"
TABLE_APARELHO = f"{PROJECT}.{DATASET}.dim_aparelho"
TABLE_EVENTOS  = f"{PROJECT}.{DATASET}.f_chip_aparelho"


class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # =======================
    # CONSULTAS PRINCIPAIS
    # =======================
    def _run(self, sql):
        try:
            return self.client.query(sql).to_dataframe()
        except Exception as e:
            print("ERRO SQL:", e)
            raise e

    def get_view(self):
        sql = f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            ORDER BY sk_chip DESC
        """
        return self._run(sql)

    def get_chips(self):
        sql = f"""
            SELECT *
            FROM `{TABLE_CHIP}`
            ORDER BY sk_chip DESC
        """
        return self._run(sql)

    def get_aparelhos(self):
        sql = f"""
            SELECT *
            FROM `{TABLE_APARELHO}`
            ORDER BY nome
        """
        return self._run(sql)

    def get_kpis(self):
        sql = f"""
            SELECT
              (SELECT COUNT(*) FROM `{TABLE_CHIP}` WHERE ativo = TRUE) AS chips_ativos,
              (SELECT COUNT(*) FROM `{TABLE_CHIP}`) AS total_chips,
              (SELECT COUNT(*) FROM `{TABLE_APARELHO}`) AS total_aparelhos,
              (SELECT COUNT(*) FROM `{TABLE_EVENTOS}`) AS total_eventos
        """
        df = self._run(sql)
        return df.iloc[0].to_dict()

    def get_ultimas_recargas(self):
        sql = f"""
            SELECT
                numero,
                operadora,
                ultima_recarga_data,
                ultima_recarga_valor
            FROM `{TABLE_CHIP}`
            WHERE ultima_recarga_data IS NOT NULL
            ORDER BY ultima_recarga_data DESC
            LIMIT 10
        """
        return self._run(sql)

    # =======================
    # INSERIR APARELHO
    # =======================
    def insert_aparelho(self, data: dict):

        row = [{
            "nome": data.get("nome"),
            "marca": data.get("marca"),
            "modelo": data.get("modelo"),
            "imei": data.get("imei"),
            "status": data.get("status"),
            "create_at": datetime.datetime.utcnow(),
            "update_at": datetime.datetime.utcnow()
        }]

        errors = self.client.insert_rows_json(TABLE_APARELHO, row)

        if errors:
            print("ERRO INSERT APARELHO:", errors)
            raise Exception(errors)

        return True

    # =======================
    # INSERIR CHIP
    # =======================
    def insert_chip(self, data: dict):

        # Converte campos numéricos
        def to_float(v):
            try:
                return float(v) if v not in ["", None] else None
            except:
                return None

        def to_int(v):
            try:
                return int(v) if v not in ["", None] else None
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
            print("ERRO INSERT CHIP:", errors)
            raise Exception(errors)

        return True

    # =======================
    # INSERIR EVENTO (opcional)
    # =======================
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
            print("ERRO INSERT EVENTO:", errors)
            raise Exception(errors)

        return True
