from google.cloud import bigquery

PROJECT = "painel-universidade"
DATASET = "marts"

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ------------------------------
    # DASHBOARD
    # ------------------------------
    def dashboard_data(self):
        query = f"""
            SELECT
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_chip`) AS total_chips,
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.dim_aparelho`) AS total_aparelhos,
                (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.f_chip_aparelho`) AS total_vinculos
        """
        return list(self.client.query(query).result())[0]

    # ------------------------------
    # LISTAGEM
    # ------------------------------

    def get_chips(self):
        query = f"SELECT * FROM `{PROJECT}.{DATASET}.dim_chip` ORDER BY numero_chip"
        return list(self.client.query(query).result())

    def get_aparelhos(self):
        query = f"SELECT * FROM `{PROJECT}.{DATASET}.dim_aparelho` ORDER BY nome_aparelho"
        return list(self.client.query(query).result())

    # ------------------------------
    # CHIPS
    # ------------------------------

    def insert_chip(self, data):
        query = f"""
            INSERT INTO `{PROJECT}.{DATASET}.dim_chip`
            (sk_chip, iccid, numero_chip, operadora, status_chip, data_ativacao)
            VALUES (
                {data['sk_chip']},
                '{data['iccid']}',
                '{data['numero_chip']}',
                '{data['operadora']}',
                '{data['status_chip']}',
                DATE('{data['data_ativacao']}')
            )
        """
        self.client.query(query)

    def update_chip(self, data):
        query = f"""
            UPDATE `{PROJECT}.{DATASET}.dim_chip`
            SET iccid = '{data['iccid']}',
                numero_chip = '{data['numero_chip']}',
                operadora = '{data['operadora']}',
                status_chip = '{data['status_chip']}'
            WHERE sk_chip = {data['sk_chip']}
        """
        self.client.query(query)

    # ------------------------------
    # ÚLTIMA RECARGA
    # ------------------------------

    def update_recarga(self, data):
        query = f"""
            UPDATE `{PROJECT}.{DATASET}.dim_chip`
            SET data_ultima_recarga = DATE('{data["data_ultima_recarga"]}'),
                valor_ultima_recarga = {data["valor_ultima_recarga"]}
            WHERE sk_chip = {data["sk_chip"]}
        """
        self.client.query(query)

    # ------------------------------
    # APARELHOS
    # ------------------------------

    def insert_aparelho(self, data):
        query = f"""
            INSERT INTO `{PROJECT}.{DATASET}.dim_aparelho`
            (sk_aparelho, nome_aparelho, imei, modelo, status_aparelho)
            VALUES (
                {data['sk_aparelho']},
                '{data['nome_aparelho']}',
                '{data['imei']}',
                '{data['modelo']}',
                '{data['status_aparelho']}'
            )
        """
        self.client.query(query)

    def update_aparelho(self, data):
        query = f"""
            UPDATE `{PROJECT}.{DATASET}.dim_aparelho`
            SET nome_aparelho = '{data['nome_aparelho']}',
                imei = '{data['imei']}',
                modelo = '{data['modelo']}',
                status_aparelho = '{data['status_aparelho']}'
            WHERE sk_aparelho = {data['sk_aparelho']}
        """
        self.client.query(query)

    # ------------------------------
    # RELAÇÃO CHIP/APARELHO
    # ------------------------------

    def vincular_chip_aparelho(self, data):
        query = f"""
            INSERT INTO `{PROJECT}.{DATASET}.f_chip_aparelho`
            (sk_chip, sk_aparelho, data_vinculo)
            VALUES (
                {data['sk_chip']},
                {data['sk_aparelho']},
                DATE('{data['data_vinculo']}')
            )
        """
        self.client.query(query)

    def desvincular_chip_aparelho(self, data):
        query = f"""
            UPDATE `{PROJECT}.{DATASET}.f_chip_aparelho`
            SET data_desvinculo = DATE('{data['data_desvinculo']}')
            WHERE sk_chip = {data['sk_chip']}
              AND sk_aparelho = {data['sk_aparelho']}
        """
        self.client.query(query)
