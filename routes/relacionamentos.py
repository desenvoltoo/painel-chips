# routes/relacionamentos.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# ============================
# Regras de capacidade
# ============================
def capacidade_por_marca(marca: str) -> int:
    m = (marca or "").strip().upper()
    # regra que você definiu: Motorola = 4 business + 8 normais = 12
    if m == "MOTOROLA":
        return 12
    # demais: 1 business + 2 normais = 3
    return 3


def norm_tipo_whatsapp(v: str) -> str:
    t = (v or "").strip()
    if not t:
        return "NORMAL"
    # deixa padronizado, mas sem inventar valores
    up = t.upper()
    if "BUS" in up:
        return "BUSINESS"
    if "NOR" in up:
        return "NORMAL"
    return t  # mantém se vier algo diferente


# ============================
# HOME
# ============================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

        # ---- chips livres (SEM aparelho) -> lista global pra TODOS
        if not chips_df.empty and "sk_aparelho_atual" in chips_df.columns:
            livres_df = chips_df[
                chips_df["sk_aparelho_atual"].isna()
                | (chips_df["sk_aparelho_atual"].astype(str).str.strip() == "")
            ].copy()
        else:
            livres_df = pd.DataFrame()

        chips_livres = []
        if not livres_df.empty:
            for _, r in livres_df.iterrows():
                chips_livres.append({
                    "sk_chip": int(r.get("sk_chip")),
                    "numero": r.get("numero"),
                    "operadora": r.get("operadora"),
                    "tipo_whatsapp": norm_tipo_whatsapp(r.get("tipo_whatsapp")),
                })

        aparelhos = []

        for _, a in aparelhos_df.iterrows():
            sk_ap = int(a.get("sk_aparelho"))
            marca = a.get("marca")
            modelo = a.get("modelo")

            cap_total = capacidade_por_marca(marca)

            # slots 1..cap_total
            slots = [{"slot": i, "chip": None} for i in range(1, cap_total + 1)]

            # ---- chips vinculados nesse aparelho (para preencher os slots)
            if not chips_df.empty and "sk_aparelho_atual" in chips_df.columns:
                vinc_df = chips_df[
                    (~chips_df["sk_aparelho_atual"].isna())
                    & (chips_df["sk_aparelho_atual"].astype("Int64") == sk_ap)
                ].copy()
            else:
                vinc_df = pd.DataFrame()

            if not vinc_df.empty:
                for _, r in vinc_df.iterrows():
                    slot = r.get("slot_whatsapp")
                    if pd.isna(slot):
                        continue
                    try:
                        idx = int(slot) - 1
                    except:
                        continue
                    if 0 <= idx < len(slots):
                        slots[idx]["chip"] = {
                            "sk_chip": int(r.get("sk_chip")),
                            "numero": r.get("numero"),
                            "operadora": r.get("operadora"),
                            "tipo_whatsapp": norm_tipo_whatsapp(r.get("tipo_whatsapp")),
                        }

            aparelhos.append({
                "sk_aparelho": sk_ap,
                "marca": marca,
                "modelo": modelo,
                "capacidade_total": cap_total,
                "slots": slots,
                "chips_sem_slot": chips_livres,  # <- GLOBAL pra todos
            })

        return render_template("relacionamentos.html", aparelhos=aparelhos)

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================
# VINCULAR / TROCAR (MOVE)
# ============================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True) or {}

        sk_chip = data.get("sk_chip")
        sk_aparelho = data.get("sk_aparelho")
        slot = data.get("slot")

        if not sk_chip or not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "sk_chip, sk_aparelho e slot são obrigatórios"}), 400

        # ✅ ajuste aqui se sua tabela real não for dim_chip
        tabela = f"`{bq.project}.marts.dim_chip`"

        # 1) Desocupa o slot alvo (se já tiver outro chip nele)
        sql_desocupar_slot = f"""
        UPDATE {tabela}
        SET sk_aparelho_atual = NULL,
            slot_whatsapp = NULL
        WHERE sk_aparelho_atual = @sk_aparelho
          AND slot_whatsapp = @slot
        """

        # 2) Move o chip selecionado pro novo slot/aparelho
        sql_mover_chip = f"""
        UPDATE {tabela}
        SET sk_aparelho_atual = @sk_aparelho,
            slot_whatsapp = @slot
        WHERE sk_chip = @sk_chip
        """

        params = [
            bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip)),
            bigquery.ScalarQueryParameter("sk_aparelho", "INT64", int(sk_aparelho)),
            bigquery.ScalarQueryParameter("slot", "INT64", int(slot)),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        bq.client.query(sql_desocupar_slot, job_config=job_config).result()
        bq.client.query(sql_mover_chip, job_config=job_config).result()

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
        sk_chip = data.get("sk_chip")

        if not sk_chip:
            return jsonify({"ok": False, "error": "sk_chip obrigatório"}), 400

        tabela = f"`{bq.project}.marts.dim_chip`"

        sql = f"""
        UPDATE {tabela}
        SET sk_aparelho_atual = NULL,
            slot_whatsapp = NULL
        WHERE sk_chip = @sk_chip
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip))]
        )

        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
