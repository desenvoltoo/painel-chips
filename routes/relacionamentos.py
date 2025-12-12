# routes/relacionamentos.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

rel_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# -------------------------------------------------------------------
# LISTAR RELACIONAMENTOS – APARELHOS + SLOTS + CHIPS DISPONÍVEIS
# -------------------------------------------------------------------
@rel_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        sql = f"""
        SELECT
          sk_aparelho,
          marca,
          modelo,
          capacidade_whatsapp,
          slot,
          sk_chip,
          numero,
          operadora
        FROM `{bq.project}.marts.vw_relacionamentos_whatsapp`
        ORDER BY sk_aparelho, slot
        """

        print("\n🔥 SQL RELACIONAMENTOS EXECUTANDO:")
        print(sql)

        df = bq.execute_query(sql)     # <-- CORREÇÃO: execute_query é o método correto
        df = sanitize_df(df)

        if df.empty:
            return render_template("relacionamentos.html", aparelhos=[])

        aparelhos_map = {}

        # -------------------------------------------------------------
        # 1 — CRIA APARELHOS E PRÉ-CRIA TODOS OS SLOTS
        # -------------------------------------------------------------
        for _, r in df.iterrows():
            sk_ap = int(r["sk_aparelho"])

            if sk_ap not in aparelhos_map:
                capacidade = int(r["capacidade_whatsapp"])

                aparelhos_map[sk_ap] = {
                    "sk_aparelho": sk_ap,
                    "marca": r["marca"],
                    "modelo": r["modelo"],
                    "capacidade_total": capacidade,
                    "slots": [{"slot": s, "chip": None} for s in range(1, capacidade + 1)],
                    "chips_sem_slot": []
                }

        # -------------------------------------------------------------
        # 2 — DISTRIBUI CHIPS EM SLOTS OU COMO DISPONÍVEIS
        # -------------------------------------------------------------
        for _, r in df.iterrows():
            sk_ap = int(r["sk_aparelho"])
            aparelho = aparelhos_map[sk_ap]

            sk_chip = r.get("sk_chip")
            if pd.isna(sk_chip):
                continue

            chip = {
                "sk_chip": int(sk_chip),
                "numero": r["numero"],
                "operadora": r["operadora"]
            }

            slot = r.get("slot")

            if pd.isna(slot) or slot == 0:
                aparelho["chips_sem_slot"].append(chip)
            else:
                idx = int(slot) - 1
                if 0 <= idx < aparelho["capacidade_total"]:
                    aparelho["slots"][idx]["chip"] = chip

        aparelhos_list = list(aparelhos_map.values())

        return render_template("relacionamentos.html", aparelhos=aparelhos_list)

    except Exception as e:
        print("❌ ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# -------------------------------------------------------------------
# VINCULAR CHIP A SLOT
# -------------------------------------------------------------------
@rel_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        d = request.get_json(force=True)

        sk_chip = d.get("sk_chip")
        sk_aparelho = d.get("sk_aparelho")
        slot = d.get("slot")

        if not sk_chip or not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "Dados faltando"}), 400

        tabela = f"`{bq.project}.marts.relacionamento_whatsapp_chip`"

        sql = f"""
        UPDATE {tabela}
        SET 
          sk_aparelho = @sk_aparelho,
          slot        = @slot
        WHERE sk_chip = @sk_chip
        """

        params = [
            bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip)),
            bigquery.ScalarQueryParameter("sk_aparelho", "INT64", int(sk_aparelho)),
            bigquery.ScalarQueryParameter("slot", "INT64", int(slot)),
        ]

        bq.client.query(sql, bigquery.QueryJobConfig(query_parameters=params)).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("❌ ERRO AO VINCULAR CHIP:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# -------------------------------------------------------------------
# DESVINCULAR CHIP DO SLOT
# -------------------------------------------------------------------
@rel_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        d = request.get_json(force=True)

        sk_chip = d.get("sk_chip")

        if not sk_chip:
            return jsonify({"ok": False, "error": "sk_chip faltando"}), 400

        tabela = f"`{bq.project}.marts.relacionamento_whatsapp_chip`"

        sql = f"""
        UPDATE {tabela}
        SET 
          sk_aparelho = NULL,
          slot        = NULL
        WHERE sk_chip = @sk_chip
        """

        params = [
            bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip))
        ]

        bq.client.query(sql, bigquery.QueryJobConfig(query_parameters=params)).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("❌ ERRO AO DESVINCULAR CHIP:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
