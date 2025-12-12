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
# LISTAR RELACIONAMENTOS – MONTA APARELHOS + SLOTS + CHIPS DISPONÍVEIS
# -------------------------------------------------------------------
@rel_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        # 🔹 VIEW COM TODOS OS RELACIONAMENTOS
        # ajuste o nome se estiver diferente aí
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
        ORDER BY marca, modelo, sk_aparelho, slot
        """

        print("🔥 SQL EXECUTANDO:")
        print(sql)

        df = bq.query(sql)          # BigQueryClient.query → retorna DataFrame
        df = sanitize_df(df)

        if df.empty:
            return render_template("relacionamentos.html", aparelhos=[])

        aparelhos_map = {}

        # 1ª passada: cria aparelhos e pré-cria os slots (1..capacidade)
        for _, row in df.iterrows():
            sk_ap = int(row["sk_aparelho"])

            if sk_ap not in aparelhos_map:
                capacidade = int(
                    row.get("capacidade_whatsapp")
                    or row.get("capacidade_total")
                    or 0
                )

                aparelho_dict = {
                    "sk_aparelho": sk_ap,
                    "marca": row["marca"],
                    "modelo": row["modelo"],
                    "capacidade_total": capacidade,
                    "slots": [],
                    "chips_sem_slot": []
                }

                # pré-cria todos os slots
                for s in range(1, capacidade + 1):
                    aparelho_dict["slots"].append({
                        "slot": s,
                        "chip": None
                    })

                aparelhos_map[sk_ap] = aparelho_dict

        # 2ª passada: encaixa os chips nos slots OU na lista de sem slot
        for _, row in df.iterrows():
            sk_ap = int(row["sk_aparelho"])
            aparelho = aparelhos_map[sk_ap]

            sk_chip = row.get("sk_chip")
            if pd.isna(sk_chip):
                # linha sem chip, ignora
                continue

            chip = {
                "sk_chip": int(sk_chip),
                "numero": row["numero"],
                "operadora": row["operadora"]
            }

            slot_num = row.get("slot")

            if pd.isna(slot_num) or slot_num in (0, ""):
                # chip sem slot definido → vai para lista de disponíveis
                aparelho["chips_sem_slot"].append(chip)
            else:
                # chip já vinculado a um slot → coloca no slot correto
                idx = int(slot_num) - 1
                if 0 <= idx < len(aparelho["slots"]):
                    aparelho["slots"][idx]["chip"] = chip

        aparelhos_list = list(aparelhos_map.values())

        return render_template(
            "relacionamentos.html",
            aparelhos=aparelhos_list
        )

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# -------------------------------------------------------------------
# VINCULAR CHIP A SLOT
# -------------------------------------------------------------------
@rel_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True) or {}

        sk_chip = data.get("sk_chip")
        sk_aparelho = data.get("sk_aparelho")
        slot = data.get("slot")

        if not sk_chip or not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "Parâmetros obrigatórios faltando"}), 400

        # 🔹 AJUSTE AQUI O NOME DA SUA TABELA DE RELACIONAMENTO
        tabela = f"`{bq.project}.marts.relacionamento_whatsapp_chip`"

        sql = f"""
        UPDATE {tabela}
        SET
          sk_aparelho = @sk_aparelho,
          slot        = @slot
        WHERE sk_chip = @sk_chip
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip)),
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", int(sk_aparelho)),
                bigquery.ScalarQueryParameter("slot", "INT64", int(slot)),
            ]
        )

        bq.client.query(sql, job_config=job_config).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# -------------------------------------------------------------------
# DESVINCULAR CHIP DO SLOT (LIMPAR SLOT)
# -------------------------------------------------------------------
@rel_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True) or {}

        sk_aparelho = data.get("sk_aparelho")
        slot = data.get("slot")

        if not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "Parâmetros obrigatórios faltando"}), 400

        # 🔹 MESMA TABELA DO UPDATE ACIMA
        tabela = f"`{bq.project}.marts.relacionamento_whatsapp_chip`"

        sql = f"""
        UPDATE {tabela}
        SET
          sk_aparelho = NULL,
          slot        = NULL
        WHERE sk_aparelho = @sk_aparelho
          AND slot        = @slot
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", int(sk_aparelho)),
                bigquery.ScalarQueryParameter("slot", "INT64", int(slot)),
            ]
        )

        bq.client.query(sql, job_config=job_config).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
