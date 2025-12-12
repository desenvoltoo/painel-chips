# routes/relacionamentos.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# ============================================================
# LISTAR RELACIONAMENTOS
# ============================================================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        sql = f"""
        SELECT
            a.sk_aparelho,
            a.marca,
            a.modelo,
            a.capacidade_whatsapp AS capacidade_total,

            c.sk_chip,
            c.numero,
            c.operadora,
            c.tipo_whatsapp,
            c.sk_aparelho_atual,
            c.slot_whatsapp

        FROM `{bq.project}.marts.dim_aparelho` a
        LEFT JOIN `{bq.project}.marts.dim_chip` c
            ON c.sk_aparelho_atual = a.sk_aparelho

        ORDER BY a.marca, a.modelo, c.slot_whatsapp
        """

        df = bq.client.query(sql).to_dataframe()
        df = sanitize_df(df)

        aparelhos = {}
        chips_livres = []

        # ------------------------------------------------
        # Primeiro: criar aparelhos + slots vazios
        # ------------------------------------------------
        for _, r in df.iterrows():
            sk = int(r["sk_aparelho"])

            if sk not in aparelhos:
                capacidade = int(r["capacidade_total"] or 0)

                aparelhos[sk] = {
                    "sk_aparelho": sk,
                    "marca": r["marca"],
                    "modelo": r["modelo"],
                    "capacidade_total": capacidade,
                    "slots": [
                        {"slot": i + 1, "chip": None}
                        for i in range(capacidade)
                    ],
                    "chips_sem_slot": []
                }

        # ------------------------------------------------
        # Segundo: classificar chips
        # ------------------------------------------------
        for _, r in df.iterrows():

            if pd.isna(r["sk_chip"]):
                continue

            chip = {
                "sk_chip": int(r["sk_chip"]),
                "numero": r["numero"],
                "operadora": r["operadora"],
                "tipo_whatsapp": r["tipo_whatsapp"]
            }

            if pd.isna(r["sk_aparelho_atual"]) or pd.isna(r["slot_whatsapp"]):
                chips_livres.append(chip)
            else:
                sk_ap = int(r["sk_aparelho_atual"])
                slot = int(r["slot_whatsapp"]) - 1

                if sk_ap in aparelhos and 0 <= slot < len(aparelhos[sk_ap]["slots"]):
                    aparelhos[sk_ap]["slots"][slot]["chip"] = chip

        # ------------------------------------------------
        # Chips livres aparecem PARA TODOS os aparelhos
        # ------------------------------------------------
        for ap in aparelhos.values():
            ap["chips_sem_slot"] = chips_livres

        return render_template(
            "relacionamentos.html",
            aparelhos=list(aparelhos.values())
        )

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# VINCULAR CHIP
# ============================================================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def vincular():
    data = request.get_json(force=True)

    sk_chip = data.get("sk_chip")
    sk_aparelho = data.get("sk_aparelho")
    slot = data.get("slot")

    if not sk_chip or not sk_aparelho or not slot:
        return jsonify({"ok": False}), 400

    sql = f"""
    UPDATE `{bq.project}.marts.dim_chip`
    SET
        sk_aparelho_atual = @sk_aparelho,
        slot_whatsapp     = @slot
    WHERE sk_chip = @sk_chip
    """

    job = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip)),
            bigquery.ScalarQueryParameter("sk_aparelho", "INT64", int(sk_aparelho)),
            bigquery.ScalarQueryParameter("slot", "INT64", int(slot)),
        ]
    )

    bq.client.query(sql, job_config=job).result()
    return jsonify({"ok": True})


# ============================================================
# DESVINCULAR CHIP
# ============================================================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def desvincular():
    data = request.get_json(force=True)
    sk_chip = data.get("sk_chip")

    if not sk_chip:
        return jsonify({"ok": False}), 400

    sql = f"""
    UPDATE `{bq.project}.marts.dim_chip`
    SET
        sk_aparelho_atual = NULL,
        slot_whatsapp     = NULL
    WHERE sk_chip = @sk_chip
    """

    job = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip)),
        ]
    )

    bq.client.query(sql, job_config=job).result()
    return jsonify({"ok": True})
