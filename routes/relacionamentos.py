# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# ============================
# Utilitário seguro
# ============================
def to_int(v):
    try:
        if v is None:
            return None
        v = str(v).strip()
        if v == "":
            return None
        return int(v)
    except:
        return None


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
                sk_chip = to_int(c.get("sk_chip"))
                if sk_chip is None:
                    continue

                chips_livres.append({
                    "sk_chip": sk_chip,
                    "numero": c.get("numero"),
                    "operadora": c.get("operadora"),
                    "tipo_whatsapp": norm_tipo_whatsapp(c.get("tipo_whatsapp")),
                })

        aparelhos = []

        # -----------------------------
        # Loop aparelhos
        # -----------------------------
        for _, a in aparelhos_df.iterrows():

            sk_aparelho = to_int(a.get("sk_aparelho"))
            capacidade = to_int(a.get("capacidade_whatsapp"))

            # ignora registros inválidos
            if sk_aparelho is None or capacidade is None:
                continue

            marca = a.get("marca")
            modelo = a.get("modelo")

            # cria slots vazios
            slots = [{"slot": i, "chip": None} for i in range(1, capacidade + 1)]

            # chips vinculados a este aparelho
            vinculados = chips_df[
                chips_df["sk_aparelho"].astype(str).str.strip() == str(sk_aparelho)
            ]

            for _, c in vinculados.iterrows():
                slot = to_int(c.get("slot_whatsapp"))
                if slot is None:
                    continue

                idx = slot - 1
                if 0 <= idx < len(slots):
                    slots[idx]["chip"] = {
                        "sk_chip": to_int(c.get("sk_chip")),
                        "numero": c.get("numero"),
                        "operadora": c.get("operadora"),
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
        data = request.get_json(force=True) or {}

        sk_chip = to_int(data.get("sk_chip"))
        sk_aparelho = to_int(data.get("sk_aparelho"))
        slot = to_int(data.get("slot"))

        if not sk_chip or not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "Dados inválidos"}), 400

        tabela = f"`{bq.project}.marts.dim_chip`"

        sql = f"""
        UPDATE {tabela}
        SET sk_aparelho = @sk_aparelho,
            slot_whatsapp = @slot
        WHERE sk_chip = @sk_chip
        """

        bq.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                    bigquery.ScalarQueryParameter("sk_aparelho", "INT64", sk_aparelho),
                    bigquery.ScalarQueryParameter("slot", "INT64", slot),
                ]
            )
        ).result()

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
        data = request.get_json(force=True) or {}
        sk_chip = to_int(data.get("sk_chip"))

        if not sk_chip:
            return jsonify({"ok": False, "error": "sk_chip inválido"}), 400

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
