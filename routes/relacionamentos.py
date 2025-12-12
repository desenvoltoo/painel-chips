# routes/relacionamentos.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import pandas as pd

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# -----------------------------
# Helpers
# -----------------------------
def _cap_por_marca(marca: str):
    m = (marca or "").strip().upper()
    if m == "MOTOROLA":
        return 4, 8   # business, normal
    return 1, 2


def _norm_tipo(tipo: str):
    t = (tipo or "").strip().upper()
    if "BUS" in t:
        return "BUSINESS"
    if "NOR" in t:
        return "NORMAL"
    # fallback: se vier vazio, assume NORMAL
    return "NORMAL"


# -------------------------------------------------------------------
# HOME
# -------------------------------------------------------------------
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        # 1) Aparelhos
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))
        if aparelhos_df.empty:
            return render_template("relacionamentos.html", aparelhos=[])

        # 2) Chips (com estado atual)
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

        # chips livres (aparecem em TODOS os aparelhos)
        chips_livres = chips_df[
            chips_df["sk_aparelho_atual"].isna() | (chips_df["sk_aparelho_atual"] == "")
        ].copy()

        def chip_dict(r):
            return {
                "sk_chip": int(r["sk_chip"]),
                "numero": r.get("numero"),
                "operadora": r.get("operadora"),
                "tipo_whatsapp": _norm_tipo(r.get("tipo_whatsapp")),
            }

        chips_livres_list = [chip_dict(r) for _, r in chips_livres.iterrows()]

        # 3) Montar cards
        aparelhos_list = []
        for _, a in aparelhos_df.iterrows():
            sk_ap = int(a["sk_aparelho"])
            marca = a.get("marca")
            modelo = a.get("modelo")

            cap_b, cap_n = _cap_por_marca(marca)

            slots_business = [{"slot": i, "chip": None} for i in range(1, cap_b + 1)]
            slots_normal   = [{"slot": i, "chip": None} for i in range(1, cap_n + 1)]

            # 4) encaixar chips já vinculados nesse aparelho
            vinc = chips_df[
                (~chips_df["sk_aparelho_atual"].isna()) &
                (chips_df["sk_aparelho_atual"].astype("Int64") == sk_ap)
            ].copy()

            for _, r in vinc.iterrows():
                tipo = _norm_tipo(r.get("tipo_whatsapp"))
                slot = r.get("slot_whatsapp")

                if pd.isna(slot) or int(slot) <= 0:
                    continue

                d = chip_dict(r)
                idx = int(slot) - 1

                if tipo == "BUSINESS":
                    if 0 <= idx < len(slots_business):
                        slots_business[idx]["chip"] = d
                else:
                    if 0 <= idx < len(slots_normal):
                        slots_normal[idx]["chip"] = d

            aparelhos_list.append({
                "sk_aparelho": sk_ap,
                "marca": marca,
                "modelo": modelo,
                "cap_business": cap_b,
                "cap_normal": cap_n,
                "slots_business": slots_business,
                "slots_normal": slots_normal,
                "chips_livres": chips_livres_list,  # <- mesma lista pra todos
            })

        return render_template("relacionamentos.html", aparelhos=aparelhos_list)

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# -------------------------------------------------------------------
# VINCULAR / MOVER CHIP
# -------------------------------------------------------------------
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True) or {}

        sk_chip = data.get("sk_chip")
        sk_aparelho = data.get("sk_aparelho")
        slot = data.get("slot")
        tipo_whatsapp = _norm_tipo(data.get("tipo_whatsapp"))

        if not sk_chip or not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "Parâmetros obrigatórios faltando"}), 400

        # ✅ tabela real onde ficam sk_aparelho_atual/slot_whatsapp/tipo_whatsapp
        # (pelo seu print: isso está nos chips)
        tabela = f"`{bq.project}.marts.dim_chip`"

        sql = f"""
        UPDATE {tabela}
        SET
          sk_aparelho_atual = @sk_aparelho,
          slot_whatsapp     = @slot,
          tipo_whatsapp     = @tipo_whatsapp
        WHERE sk_chip = @sk_chip
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sk_chip", "INT64", int(sk_chip)),
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", int(sk_aparelho)),
                bigquery.ScalarQueryParameter("slot", "INT64", int(slot)),
                bigquery.ScalarQueryParameter("tipo_whatsapp", "STRING", tipo_whatsapp),
            ]
        )

        bq.client.query(sql, job_config=job_config).result()
        return jsonify({"ok": True})

    except Exception as e:
        print("ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# -------------------------------------------------------------------
# DESVINCULAR CHIP (por sk_chip)
# -------------------------------------------------------------------
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
        SET
          sk_aparelho_atual = NULL,
          slot_whatsapp     = NULL
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
