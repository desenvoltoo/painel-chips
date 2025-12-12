# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

rel_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()

# ============================================================
# LISTAGEM COMPLETA — APARELHOS + SLOTS + TODOS OS CHIPS LIVRES
# ============================================================
@rel_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        sql = f"""
        SELECT
            ap.sk_aparelho,
            ap.marca,
            ap.modelo,
            ap.capacidade_whatsapp AS capacidade_total,

            sl.slot,
            ch.sk_chip,
            ch.numero,
            ch.operadora,
            ch.tipo_whatsapp,
            ch.sk_aparelho_atual,
            ch.slot_whatsapp
        FROM `{bq.project}.marts.vw_relacionamentos_whatsapp` ap
        LEFT JOIN `{bq.project}.marts.relacionamento_whatsapp_chip` sl
            ON sl.sk_aparelho = ap.sk_aparelho
        LEFT JOIN `{bq.project}.marts.dim_chip` ch
            ON ch.sk_aparelho_atual = ap.sk_aparelho
           AND ch.slot_whatsapp = sl.slot
        ORDER BY ap.marca, ap.modelo, ap.sk_aparelho, sl.slot
        """

        df = bq.query(sql)
        df = sanitize_df(df)

        # ---------------------------------------------
        # BUSCAR TODOS OS CHIPS DISPONÍVEIS GLOBAIS
        # ---------------------------------------------
        sql_chips_livres = f"""
        SELECT 
            sk_chip, numero, operadora, tipo_whatsapp
        FROM `{bq.project}.marts.dim_chip`
        WHERE sk_aparelho_atual IS NULL
           OR slot_whatsapp IS NULL
        ORDER BY numero
        """

        chips_livres = bq.query(sql_chips_livres)
        chips_livres = sanitize_df(chips_livres)

        # ---------------------------------------------
        # MONTAR APARELHOS COM SLOTS
        # ---------------------------------------------
        aparelhos_map = {}

        for _, row in df.iterrows():
            sk_ap = int(row["sk_aparelho"])

            if sk_ap not in aparelhos_map:
                capacidade = int(row["capacidade_total"] or 0)

                aparelhos_map[sk_ap] = {
                    "sk_aparelho": sk_ap,
                    "marca": row["marca"],
                    "modelo": row["modelo"],
                    "capacidade_total": capacidade,
                    "slots": [],
                    "chips_sem_slot": chips_livres.to_dict(orient="records")  # 🔥 LISTA GLOBAL
                }

                for s in range(1, capacidade + 1):
                    aparelhos_map[sk_ap]["slots"].append({
                        "slot": s,
                        "chip": None
                    })

        # Colocar chips nos slots
        for _, row in df.iterrows():
            sk_ap = int(row["sk_aparelho"])
            slot = int(row["slot"])
            aparelho = aparelhos_map[sk_ap]

            if not pd.isna(row["sk_chip"]):
                chip = {
                    "sk_chip": int(row["sk_chip"]),
                    "numero": row["numero"],
                    "operadora": row["operadora"],
                    "tipo_whatsapp": row.get("tipo_whatsapp")
                }
                aparelho["slots"][slot - 1]["chip"] = chip

        return render_template("relacionamentos.html", aparelhos=list(aparelhos_map.values()))

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return str(e), 500


# ============================================================
# VINCULAR CHIP → APARELHO + SLOT
# ============================================================
@rel_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json()

        sk_chip = int(data["sk_chip"])
        sk_aparelho = int(data["sk_aparelho"])
        slot = int(data["slot"])

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET 
            sk_aparelho_atual = @ap,
            slot_whatsapp = @slot
        WHERE sk_chip = @chip
        """

        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ap", "INT64", sk_aparelho),
                bigquery.ScalarQueryParameter("slot", "INT64", slot),
                bigquery.ScalarQueryParameter("chip", "INT64", sk_chip),
            ]
        )

        bq.client.query(sql, config).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO AO VINCULAR:", e)
        return jsonify({"ok": False, "erro": str(e)}), 500


# ============================================================
# DESVINCULAR CHIP (DE QUALQUER APARELHO)
# ============================================================
@rel_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json()
        sk_chip = int(data["sk_chip"])

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET 
            sk_aparelho_atual = NULL,
            slot_whatsapp = NULL
        WHERE sk_chip = @chip
        """

        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("chip", "INT64", sk_chip),
            ]
        )

        bq.client.query(sql, config).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "erro": str(e)}), 500
