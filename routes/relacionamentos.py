# routes/relacionamentos.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()

@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        # slots + chips encaixados
        df = bq.query(f"""
            SELECT
              sk_aparelho, marca, modelo, capacidade_whatsapp, slot,
              sk_chip, numero, operadora, tipo_whatsapp
            FROM `{bq.project}.marts.vw_relacionamentos_whatsapp`
            ORDER BY marca, modelo, sk_aparelho, slot
        """)
        df = sanitize_df(df)

        # chips livres (aparecem pra TODOS os aparelhos)
        livres = bq.query(f"""
            SELECT sk_chip, numero, operadora, tipo_whatsapp
            FROM `{bq.project}.marts.dim_chip`
            WHERE sk_aparelho_atual IS NULL OR slot_whatsapp IS NULL
            ORDER BY numero
        """)
        livres = sanitize_df(livres).to_dict(orient="records")

        aparelhos_map = {}

        for _, row in df.iterrows():
            sk_ap = int(row["sk_aparelho"])
            if sk_ap not in aparelhos_map:
                aparelhos_map[sk_ap] = {
                    "sk_aparelho": sk_ap,
                    "marca": row["marca"],
                    "modelo": row["modelo"],
                    "capacidade_total": int(row["capacidade_whatsapp"] or 0),
                    "slots": [],
                    "chips_sem_slot": livres,  # lista global
                }

            aparelhos_map[sk_ap]["slots"].append({
                "slot": int(row["slot"]),
                "chip": None
            })

        # encaixar chip em cada slot
        for _, row in df.iterrows():
            sk_ap = int(row["sk_aparelho"])
            slot_num = int(row["slot"])
            if row.get("sk_chip") not in (None, ""):
                chip = {
                    "sk_chip": int(row["sk_chip"]),
                    "numero": row["numero"],
                    "operadora": row["operadora"],
                    "tipo_whatsapp": row.get("tipo_whatsapp")
                }
                aparelhos_map[sk_ap]["slots"][slot_num - 1]["chip"] = chip

        return render_template("relacionamentos.html", aparelhos=list(aparelhos_map.values()))

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True) or {}
        sk_chip = int(data["sk_chip"])
        sk_aparelho = int(data["sk_aparelho"])
        slot = int(data["slot"])

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET sk_aparelho_atual = @ap,
            slot_whatsapp     = @slot
        WHERE sk_chip = @chip
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ap", "INT64", sk_aparelho),
                bigquery.ScalarQueryParameter("slot", "INT64", slot),
                bigquery.ScalarQueryParameter("chip", "INT64", sk_chip),
            ]
        )
        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True) or {}
        # ✅ seu HTML atual manda (sk_aparelho,slot), mas é mais seguro desvincular pelo chip também.
        sk_chip = data.get("sk_chip")

        if sk_chip:
            sk_chip = int(sk_chip)
            where = "WHERE sk_chip = @chip"
            params = [bigquery.ScalarQueryParameter("chip", "INT64", sk_chip)]
        else:
            sk_aparelho = int(data["sk_aparelho"])
            slot = int(data["slot"])
            where = "WHERE sk_aparelho_atual = @ap AND slot_whatsapp = @slot"
            params = [
                bigquery.ScalarQueryParameter("ap", "INT64", sk_aparelho),
                bigquery.ScalarQueryParameter("slot", "INT64", slot),
            ]

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET sk_aparelho_atual = NULL,
            slot_whatsapp     = NULL
        {where}
        """

        job_config = bigquery.QueryJobConfig(query_parameters=params)
        bq.client.query(sql, job_config=job_config).result()

        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@relacionamentos_bp.route("/relacionamentos/desvincular_todos", methods=["POST"])
def relacionamentos_desvincular_todos():
    """✅ botão/admin: limpa TODAS as vinculações"""
    try:
        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
        SET sk_aparelho_atual = NULL,
            slot_whatsapp = NULL
        WHERE sk_aparelho_atual IS NOT NULL
           OR slot_whatsapp IS NOT NULL
        """
        bq.client.query(sql).result()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
