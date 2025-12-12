# routes/aparelhos.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
import os

aparelhos_bp = Blueprint("aparelhos", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")

# =======================================================
# 📱 LISTAGEM DE APARELHOS (PAINEL)
# =======================================================
@aparelhos_bp.route("/aparelhos")
def aparelhos_list():
    """
    Fonte: vw_aparelhos

    Campos esperados:
    sk_aparelho, marca, modelo, capacidade_whatsapp, status, created_at
    """
    try:
        df = bq.get_view("vw_aparelhos")
        df = sanitize_df(df)

        # 🔒 Blindagem total para Jinja / JS
        aparelhos = (
            df
            .where(df.notna(), None)   # NaN → None
            .to_dict(orient="records")
        )

        return render_template(
            "aparelhos.html",
            aparelhos=aparelhos
        )

    except Exception as e:
        print("🚨 Erro ao carregar aparelhos:", e)
        return "Erro ao carregar aparelhos", 500


# =======================================================
# ➕ UPSERT DE APARELHO (INSERT / UPDATE)
# =======================================================
@aparelhos_bp.route("/aparelhos/add", methods=["POST"])
def aparelhos_add():
    """
    Upsert usando dim_aparelho
    """
    try:
        payload = {
            "id_aparelho": request.form.get("id_aparelho"),
            "marca": request.form.get("marca"),
            "modelo": request.form.get("modelo"),
            "imei": request.form.get("imei"),
            "status": request.form.get("status"),

            # capacidades (opcional, se vier do form)
            "qtd_whatsapp_total": request.form.get("qtd_whatsapp_total"),
            "qtd_whatsapp_business": request.form.get("qtd_whatsapp_business"),
            "qtd_whatsapp_normal": request.form.get("qtd_whatsapp_normal"),
        }

        # remove chaves vazias
        payload = {k: v for k, v in payload.items() if v not in ("", None)}

        bq.upsert_aparelho(payload)

        return redirect("/aparelhos")

    except Exception as e:
        print("🚨 Erro ao salvar aparelho:", e)
        return "Erro ao salvar aparelho", 500
