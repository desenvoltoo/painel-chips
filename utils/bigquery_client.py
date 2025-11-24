from google.cloud import bigquery
from datetime import datetime
import pytz

PROJECT = "painel-universidade"
DATASET = "marts"

def agora():
    return datetime.now(pytz.timezone("America/Sao_Paulo"))

class BigQueryClient:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT)

    # ============================================================
    # EXECUTOR SEGURO DE SQL
    # ============================================================
    def _run(self, sql):
        try:
            return self.client.query(sql).to_dataframe()
        except Exception as e:
            print("\n❌ ERRO SQL:\n", sql)
            print(e)
            raise e

    # ============================================================
    # SELECTS PRINCIPAIS
    # ============================================================
    def get_view(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        ORDER BY sk_chip
        """
        return self._run(sql)

    def get_chips(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.dim_chip`
        ORDER BY numero
        """
        return self._run(sql)

    def get_aparelhos(self):
        sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.dim_aparelho`
        ORDER BY modelo
        """
        return self._run(sql)

    # ============================================================
    # UPSERT — CHIP
    # ============================================================
    def upsert_chip(self, form):
        id_chip = form.get("id_chip")
        numero = form.get("numero")
        operadora = form.get("operadora")
        plano = form.get("plano")
        status = form.get("status")
        dt_inicio = form.get("dt_inicio")
        ultima_valor = form.get("ultima_recarga_valor") or "0"
        ultima_data = form.get("ultima_recarga_data")
        total_gasto = form.get("total_gasto") or "0"

        aparelho = form.get("sk_aparelho_atual")
        aparelho = aparelho if aparelho not in ("", None) else "NULL"

        sql = f"""
        MERGE `{PROJECT}.{DATASET}.dim_chip` T
        USING (
            SELECT
                '{id_chip}' AS id_chip,
                '{numero}' AS numero,
                '{operadora}' AS operadora,
                '{plano}' AS plano,
                '{status}' AS status,
                {f"DATE('{dt_inicio}')" if dt_inicio else "NULL"} AS dt_inicio,
                {ultima_valor} AS ultima_recarga_valor,
                {f"DATE('{ultima_data}')" if ultima_data else "NULL"} AS ultima_recarga_data,
                {total_gasto} AS total_gasto,
                {aparelho} AS sk_aparelho_atual,
                CURRENT_TIMESTAMP() AS update_at
        ) S
        ON T.id_chip = S.id_chip

        WHEN MATCHED THEN
            UPDATE SET
                numero = S.numero,
                operadora = S.operadora,
                plano = S.plano,
                status = S.status,
                dt_inicio = S.dt_inicio,
                ultima_recarga_valor = S.ultima_recarga_valor,
                ultima_recarga_data = S.ultima_recarga_data,
                total_gasto = S.total_gasto,
                sk_aparelho_atual = S.sk_aparelho_atual,
                update_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
            INSERT (
                id_chip, numero, operadora, plano, status, dt_inicio,
                ultima_recarga_valor, ultima_recarga_data,
                total_gasto, sk_aparelho_atual,
                ativo, create_at, update_at
            )
            VALUES (
                S.id_chip, S.numero, S.operadora, S.plano, S.status, S.dt_inicio,
                S.ultima_recarga_valor, S.ultima_recarga_data,
                S.total_gasto, S.sk_aparelho_atual,
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """
        self._run(sql)

    # ============================================================
    # UPSERT — APARELHO
    # ============================================================
    def upsert_aparelho(self, form):
        sk = form.get("sk_aparelho")
        modelo = form.get("modelo")
        marca = form.get("marca")
        imei = form.get("imei")
        status = form.get("status") or "ATIVO"
        id_aparelho = form.get("id_aparelho")

        sql = f"""
        MERGE `{PROJECT}.{DATASET}.dim_aparelho` T
        USING (
            SELECT
                {sk} AS sk_aparelho,
                {'NULL' if not id_aparelho else f"'{id_aparelho}'"} AS id_aparelho,
                '{modelo}' AS modelo,
                '{marca}' AS marca,
                {'NULL' if not imei else f"'{imei}'"} AS imei,
                '{status}' AS status,
                CURRENT_TIMESTAMP() AS update_at
        ) S
        ON T.sk_aparelho = S.sk_aparelho

        WHEN MATCHED THEN
            UPDATE SET
                modelo = S.modelo,
                marca = S.marca,
                imei = S.imei,
                status = S.status,
                update_at = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN
            INSERT (
                sk_aparelho, id_aparelho, modelo, marca, imei, status,
                ativo, create_at, update_at
            )
            VALUES (
                S.sk_aparelho, S.id_aparelho, S.modelo, S.marca, S.imei, S.status,
                TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
        """

        self._run(sql)

    # ============================================================
    # INSERÇÃO DE EVENTOS (mantida)
    # ============================================================
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
