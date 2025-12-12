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
# 📌 LISTAGEM COMPLETA — APARELHOS + SLOTS + CHIPS LIVRES
# ============================================================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        # 1️⃣ Aparelhos + slots + chips já vinculados
        df = bq.query(f"""
            SELECT
              sk_aparelho,
              marca,
              modelo,
              capacidade_whatsapp,
              slot,
              sk_chip,
              numero,
              operadora,
              tipo_whatsapp
            FROM `{bq.project}.marts.vw_relacionamentos_whatsapp`
            ORDER BY marca, modelo, sk_aparelho, slot
        """)
        df = sanitize_df(df)

        # 2️⃣ Chips LIVRES (GLOBAL — aparecem para TODOS os aparelhos)
        chips_livres_df = bq.query(f"""
            SELECT
              sk_chip,
              numero,
              operadora,
              tipo_whatsapp
            FROM `{bq.project}.marts.dim_chip`
            WHERE sk_aparelho_atual IS NULL
               OR slot_whatsapp IS NULL
            ORDER BY numero
        """)
        chips_livres = sanitize_df(chips_livres_df).to_dict(orient="records")

        aparelhos = {}

        # 3️⃣ Cria aparelhos + slots vazios
        for _, r in df.iterrows():
            sk_ap = int(r["sk_aparelho"])

            if sk_ap not in aparelhos:
                capacidade = int(r["capacidade_whatsapp"] or 0)

                aparelhos[sk_ap] = {
                    "sk_aparelho": sk_ap,
                    "marca": r["marca"],
                    "modelo": r["modelo"],
                    "capacidade_total": capacidade,
                    "slots": [
                        {"slot": i + 1, "chip": None}
                        for i in range(capacidade)
                    ],
                    # 🔥 GLOBAL
                    "chips_sem_slot": chips_livres
                }

        # 4️⃣ Encaixa chips nos slots corretos
        for _, r in df.iterrows():
            if pd.notna(r["sk_chip"]) and pd.notna(r["slot"]):
                sk_ap = int(r["sk_aparelho"])
                idx = int(r["slot"]) - 1

                if 0 <= idx < len(aparelhos[sk_ap]["slots"]):
                    aparelhos[sk_ap]["slots"][idx]["chip"] = {
                        "sk_chip": int(r["sk_chip"]),
                        "numero": r["numero"],
                        "operadora": r["operadora"],
                        "tipo_whatsapp": r["tipo_whatsapp"]
                    }

        return render_template(
            "relacionamentos.html",
            aparelhos=list(aparelhos.values())
        )

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# 🔄 VINCULAR CHIP → APARELHO + SLOT
# ============================================================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True)

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET
          sk_aparelho_atual = @aparelho,
          slot_whatsapp     = @slot
        WHERE sk_chip = @chip
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("chip", "INT64", int(data["sk_chip"])),
                bigquery.ScalarQueryParameter("aparelho", "INT64", int(data["sk_aparelho"])),
                bigquery.ScalarQueryParameter("slot", "INT64", int(data["slot"]))
            ]
        )

        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# ❌ DESVINCULAR CHIP
# ============================================================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True)

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET
          sk_aparelho_atual = NULL,
          slot_whatsapp     = NULL
        WHERE sk_chip = @chip
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("chip", "INT64", int(data["sk_chip"]))
            ]
        )

        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
