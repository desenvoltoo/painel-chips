# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# ============================
# Normalização WhatsApp
# ============================
def norm_tipo_whatsapp(v):
    if not v or str(v).strip() == "":
        return "NORMAL"
    v = str(v).upper()
    if "BUS" in v:
        return "BUSINESS"
    return "NORMAL"


# ============================
# HOME
# ============================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

        # -----------------------------
        # Chips livres (globais)
        # -----------------------------
        chips_livres = []

        if not chips_df.empty:
            livres_df = chips_df[
                chips_df["sk_aparelho"].isna()
                | (chips_df["sk_aparelho"].astype(str).str.strip() == "")
            ]

            for _, c in livres_df.iterrows():
                chips_livres.append({
                    "sk_chip": int(c["sk_chip"]),
                    "numero": c["numero"],
                    "operadora": c["operadora"],
                    "tipo_whatsapp": norm_tipo_whatsapp(c.get("tipo_whatsapp")),
                })

        aparelhos = []

        # -----------------------------
        # Loop aparelhos
        # -----------------------------
        for _, a in aparelhos_df.iterrows():

            sk_aparelho = int(a["sk_aparelho"])
            marca = a["marca"]
            modelo = a["modelo"]
            capacidade = int(a["capacidade_whatsapp"])

            # cria slots vazios
            slots = [{"slot": i, "chip": None} for i in range(1, capacidade + 1)]

            # chips vinculados a este aparelho
            vinculados = chips_df[
                chips_df["sk_aparelho"] == sk_aparelho
            ]

            for _, c in vinculados.iterrows():
                slot = c.get("slot_whatsapp")
                if pd.isna(slot):
                    continue

                idx = int(slot) - 1
                if 0 <= idx < len(slots):
                    slots[idx]["chip"] = {
                        "sk_chip": int(c["sk_chip"]),
                        "numero": c["numero"],
                        "operadora": c["operadora"],
                        "tipo_whatsapp": norm_tipo_whatsapp(c.get("tipo_whatsapp")),
                    }

            aparelhos.append({
                "sk_aparelho": sk_aparelho,
                "marca": marca,
                "modelo": modelo,
                "capacidade_total": capacidade,
                "slots": slots,
                "chips_sem_slot": chips_livres,  # GLOBAL
            })

        return render_template(
            "relacionamentos.html",
            aparelhos=aparelhos
        )

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================
# VINCULAR / TROCAR
# ============================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True)

        sk_chip = int(data["sk_chip"])
        sk_aparelho = int(data["sk_aparelho"])
        slot = int(data["slot"])

        tabela = f"`{bq.project}.marts.dim_chip`"

        sql = f"""
        UPDATE {tabela}
        SET sk_aparelho = @sk_aparelho,
            slot_whatsapp = @slot
        WHERE sk_chip = @sk_chip
        """

        job = bq.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                    bigquery.ScalarQueryParameter("sk_aparelho", "INT64", sk_aparelho),
                    bigquery.ScalarQueryParameter("slot", "INT64", slot),
                ]
            )
        )
        job.result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================
# DESVINCULAR
# ============================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True)
        sk_chip = int(data["sk_chip"])

        tabela = f"`{bq.project}.marts.dim_chip`"

        sql = f"""
        UPDATE {tabela}
        SET sk_aparelho = NULL,
            slot_whatsapp = NULL
        WHERE sk_chip = @sk_chip
        """

        bq.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip)
                ]
            )
        ).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
