# -*- coding: utf-8 -*-

import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT  = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET  = os.getenv("BQ_DATASET", "marts")
LOCATION = os.getenv("BQ_LOCATION", "us")


# ============================================================
# HELPERS
# ============================================================
def q(value):
    if value in [None, "", "None", "null"]:
        return "NULL"
    value = str(value).strip().replace("'", "''")
    return f"'{value}'"


def normalize_number(value):
    if value in [None, "", "None", "null"]:
        return "NULL"
    try:
        return str(float(str(value).replace(",", ".")))
    except:
        return "NULL"


def normalize_date(value):
    if value in [None, "", "None", "null"]:
        return "NULL"

    value = str(value)

    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return f"DATE('{value}')"

    if "/" in value:
        try:
            d, m, y = value.split("/")
            return f"DATE('{y}-{m.zfill(2)}-{d.zfill(2)}')"
        except:
            return "NULL"

    if "T" in value:
        return f"DATE('{value.split('T')[0]}')"

    return "NULL"


# ============================================================
# BIGQUERY CLIENT
# ============================================================
class BigQueryClient:

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT, location=LOCATION)
        self.project = PROJECT
        self.dataset = DATASET


    # --------------------------------------------------------
    def _run(self, sql: str):
        print("\n🔥 EXECUTANDO SQL:\n", sql, "\n" + "=" * 60)
        job = self.client.query(sql)
        df = job.result().to_dataframe(create_bqstorage_client=False)
        return df.astype(object).where(pd.notnull(df), None)


    # --------------------------------------------------------
    def get_view(self, view_name: str):
        return self._run(f"""
            SELECT *
            FROM `{self.project}.{self.dataset}.{view_name}`
        """)


    # ========================================================
    # UPSERT CHIP — FONTE ÚNICA DO PAINEL
    # ========================================================
    def upsert_chip(self, form: dict):

        # ---------- IDENTIFICA CHIP ----------
        sk_chip = form.get("sk_chip")

        if not sk_chip:
            sk_chip = int(self._run(f"""
                SELECT COALESCE(MAX(sk_chip),0) + 1 AS sk
                FROM `{self.project}.{self.dataset}.dim_chip`
            """).iloc[0]["sk"])
            is_new = True
            antigo = {}
        else:
            sk_chip = int(sk_chip)
            is_new = False

            antigo_df = self._run(f"""
                SELECT *
                FROM `{self.project}.{self.dataset}.dim_chip`
                WHERE sk_chip = {sk_chip}
                LIMIT 1
            """)
            antigo = antigo_df.iloc[0].to_dict() if not antigo_df.empty else {}

        # ---------- CAMPOS ----------
        numero   = q(form.get("numero"))
        operadora = q(form.get("operadora"))
        operador  = q(form.get("operador"))
        plano     = q(form.get("plano"))
        status    = q(form.get("status"))
        observacao = q(form.get("observacao"))

        dt_inicio = normalize_date(
            form.get("dt_inicio") or form.get("data_inicio")
        )

        ultima_recarga_data  = normalize_date(form.get("ultima_recarga_data"))
        ultima_recarga_valor = normalize_number(form.get("ultima_recarga_valor"))
        total_gasto          = normalize_number(form.get("total_gasto"))

        sk_aparelho_atual = form.get("sk_aparelho_atual")
        sk_aparelho_atual = (
            int(sk_aparelho_atual) if sk_aparelho_atual not in [None, "", "None"] else "NULL"
        )

        slot_whatsapp = form.get("slot_whatsapp")
        slot_whatsapp = (
            int(slot_whatsapp) if slot_whatsapp not in [None, "", "None"] else "NULL"
        )

        # ====================================================
        # INSERT
        # ====================================================
        if is_new:
            sql = f"""
                INSERT INTO `{self.project}.{self.dataset}.dim_chip` (
                    sk_chip,
                    id_chip,
                    numero,
                    operadora,
                    operador,
                    plano,
                    status,
                    dt_inicio,
                    ultima_recarga_data,
                    ultima_recarga_valor,
                    total_gasto,
                    sk_aparelho_atual,
                    slot_whatsapp,
                    observacao,
                    ativo,
                    created_at,
                    updated_at
                )
                VALUES (
                    {sk_chip},
                    {q(form.get("id_chip"))},
                    {numero},
                    {operadora},
                    {operador},
                    {plano},
                    {status},
                    {dt_inicio},
                    {ultima_recarga_data},
                    {ultima_recarga_valor},
                    {total_gasto},
                    {sk_aparelho_atual},
                    {slot_whatsapp},
                    {observacao},
                    TRUE,
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP()
                )
            """
            self._run(sql)
            return

        # ====================================================
        # UPDATE
        # ====================================================
        sql = f"""
            UPDATE `{self.project}.{self.dataset}.dim_chip`
            SET
                numero = {numero},
                operadora = {operadora},
                operador = {operador},
                plano = {plano},
                status = {status},
                dt_inicio = {dt_inicio},
                ultima_recarga_data = {ultima_recarga_data},
                ultima_recarga_valor = {ultima_recarga_valor},
                total_gasto = {total_gasto},
                sk_aparelho_atual = {sk_aparelho_atual},
                slot_whatsapp = {slot_whatsapp},
                observacao = {observacao},
                updated_at = CURRENT_TIMESTAMP()
            WHERE sk_chip = {sk_chip}
        """
        self._run(sql)

        # ====================================================
        # REGISTRA EVENTOS AUTOMÁTICOS
        # ====================================================
        for campo in [
            "status",
            "sk_aparelho_atual",
            "slot_whatsapp",
            "ultima_recarga_valor",
            "ultima_recarga_data"
        ]:
            novo = form.get(campo)
            antigo_val = antigo.get(campo)

            if str(novo) != str(antigo_val):
                self._run(f"""
                    INSERT INTO `{self.project}.{self.dataset}.f_chip_evento` (
                        sk_evento,
                        sk_chip,
                        tipo_evento,
                        campo,
                        valor_antigo,
                        valor_novo,
                        origem,
                        observacao,
                        created_at
                    )
                    VALUES (
                        (SELECT COALESCE(MAX(sk_evento),0)+1 FROM `{self.project}.{self.dataset}.f_chip_evento`),
                        {sk_chip},
                        'ALTERACAO',
                        '{campo}',
                        {q(antigo_val)},
                        {q(novo)},
                        'Painel',
                        'Atualização via painel',
                        CURRENT_TIMESTAMP()
                    )
                """)
