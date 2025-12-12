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
# REGRAS DE CAPACIDADE (CONFORME DEFINIDO)
# ============================================================
def capacidade_por_marca(marca: str) -> int:
    m = (marca or "").strip().upper()
    # Motorola = 4 Business + 8 Normal
    if m == "MOTOROLA":
        return 12
    # Demais = 1 Business + 2 Normal
    return 3


def norm_tipo_whatsapp(v: str) -> str:
    t = (v or "").strip()
    if not t:
        return "NORMAL"
    up = t.upper()
    if "BUS" in up:
        return "BUSINESS"
    if "NOR" in up:
        return "NORMAL"
    return t


# ============================================================
# HOME
# ============================================================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

        # ----------------------------
        # CHIPS LIVRES (SEM APARELHO)
        # ----------------------------
        livres_df = chips_df[
            chips_df["sk_aparelho"].isna()
        ].copy()

        chips_livres = [
            {
                "sk_chip": int(r.sk_chip),
                "numero": r.numero,
                "operadora": r.operadora,
                "tipo_whatsapp": norm_tipo_whatsapp(r.tipo_whatsapp),
            }
            for r in livres_df.itertuples()
        ]

        aparelhos = []

        for a in aparelhos_df.itertuples():
            sk_ap = int(a.sk_aparelho)
            cap_total = capacidade_por_marca(a.marca)

            # slots vazios
            slots = [{"slot": i, "chip": None} for i in range(1, cap_total + 1)]

            # ----------------------------
            # CHIPS VINCULADOS AO APARELHO
            # ----------------------------
            vinc_df = chips_df[
                chips_df["sk_aparelho"] == sk_ap
            ].copy()

            for r in vinc_df.itertuples():
                if pd.isna(r.slot_whatsapp):
                    continue
                idx = int(r.slot_whatsapp) - 1
                if 0 <= idx < cap_total:
                    slots[idx]["chip"] = {
                        "sk_chip": int(r.sk_chip),
                        "numero": r.numero,
                        "operadora": r.operadora,
                        "tipo_whatsapp": norm_tipo_whatsapp(r.tipo_whatsapp),
                    }

            aparelhos.append({
                "sk_aparelho": sk_ap,
                "marca": a.marca,
                "modelo": a.modelo,
                "capacidade_total": cap_total,
                "slots": slots,
                "chips_sem_slot": chips_livres
            })

        return render_template("relacionamentos.html", aparelhos=aparelhos)

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# VINCULAR / TROCAR CHIP (EVENTO)
# ============================================================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True) or {}

        sk_chip = int(data.get("sk_chip"))
        sk_aparelho = int(data.get("sk_aparelho"))
        slot = int(data.get("slot"))

        tabela_fato = f"`{bq.project}.marts.f_chip_aparelho`"

        sql = f"""
        INSERT INTO {tabela_fato}
        (
            sk_chip,
            sk_aparelho,
            slot_whatsapp,
            created_at
        )
        VALUES
        (
            @sk_chip,
            @sk_aparelho,
            @slot,
            CURRENT_TIMESTAMP()
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", sk_aparelho),
                bigquery.ScalarQueryParameter("slot", "INT64", slot),
            ]
        )

        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# DESVINCULAR CHIP (EVENTO)
# ============================================================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True) or {}
        sk_chip = int(data.get("sk_chip"))

        tabela_fato = f"`{bq.project}.marts.f_chip_aparelho`"

        sql = f"""
        INSERT INTO {tabela_fato}
        (
            sk_chip,
            sk_aparelho,
            slot_whatsapp,
            created_at
        )
        VALUES
        (
            @sk_chip,
            NULL,
            NULL,
            CURRENT_TIMESTAMP()
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip)
            ]
        )

        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
