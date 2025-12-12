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
        return None
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
        df = sanitize_df(
            bq.get_view("vw_relacionamentos_whatsapp")
        )

        if df.empty:
            return render_template("relacionamentos.html", aparelhos=[])

        aparelhos = []

        # -----------------------------
        # Chips livres (NÃO vinculados)
        # -----------------------------
        chips_livres = [
            {
                "sk_chip": to_int(r["sk_chip"]),
                "numero": r["numero"],
                "operadora": r["operadora"],
                "tipo_whatsapp": norm_tipo_whatsapp(r.get("tipo_whatsapp")),
            }
            for _, r in df[df["sk_aparelho_atual"].isna()].iterrows()
            if to_int(r["sk_chip"]) is not None
        ]

        # -----------------------------
        # Agrupa por aparelho
        # -----------------------------
        for sk_aparelho, g in df.groupby("sk_aparelho", dropna=True):

            sk_aparelho = to_int(sk_aparelho)
            if sk_aparelho is None:
                continue

            marca = g["marca"].iloc[0]
            modelo = g["modelo"].iloc[0]
            capacidade = to_int(g["capacidade_whatsapp"].iloc[0])

            if capacidade is None:
                continue

            # cria slots vazios
            slots = {i: None for i in range(1, capacidade + 1)}

            # chips vinculados a este aparelho
            vinculados = g[
                g["sk_aparelho_atual"].notna()
                & g["slot_whatsapp"].notna()
            ]

            for _, r in vinculados.iterrows():
                slot = to_int(r["slot_whatsapp"])
                if slot in slots:
                    slots[slot] = {
                        "sk_chip": to_int(r["sk_chip"]),
                        "numero": r["numero"],
                        "operadora": r["operadora"],
                        "tipo_whatsapp": norm_tipo_whatsapp(r.get("tipo_whatsapp")),
                    }

            aparelhos.append({
                "sk_aparelho": sk_aparelho,
                "marca": marca,
                "modelo": modelo,
                "capacidade_total": capacidade,
                "slots": [
                    {"slot": s, "chip": slots[s]}
                    for s in range(1, capacidade + 1)
                ],
                "chips_sem_slot": chips_livres
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
        SET sk_aparelho_atual = @sk_aparelho,
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
        SET sk_aparelho_atual = NULL,
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
