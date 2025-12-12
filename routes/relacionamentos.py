# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# ============================================================
# Utils
# ============================================================
def to_int(v):
    try:
        if v is None:
            return None
        v = str(v).strip()
        if v.lower() in ["", "none", "null", "nan"]:
            return None
        return int(v)
    except:
        return None


def is_null(v):
    return v is None or str(v).strip().lower() in ["none", "null", "nan"]


# ============================================================
# HOME
# ============================================================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        df = sanitize_df(bq.get_view("vw_relacionamentos_whatsapp"))

        if df.empty:
            return render_template(
                "relacionamentos.html",
                aparelhos=[],
                chips_livres=[]
            )

        # =====================================================
        # 🔹 CHIPS LIVRES (BLINDADO)
        # regra real: sk_chip existe E sk_aparelho_atual é NULL
        # =====================================================
        chips_livres = []

        for _, r in df.iterrows():
            sk_chip = to_int(r.get("sk_chip"))
            sk_aparelho_atual = r.get("sk_aparelho_atual")

            if sk_chip and is_null(sk_aparelho_atual):
                chips_livres.append({
                    "sk_chip": sk_chip,
                    "numero": r.get("numero"),
                    "operadora": r.get("operadora"),
                    "tipo_whatsapp": r.get("tipo_whatsapp") or "A DEFINIR",
                })

        # =====================================================
        # 🔹 AGRUPA POR APARELHO (IGNORA LINHAS SEM APARELHO)
        # =====================================================
        aparelhos = []

        for sk_aparelho, g in df.groupby("sk_aparelho"):
            sk_aparelho = to_int(sk_aparelho)
            if not sk_aparelho:
                continue  # ignora chips livres

            marca = g["marca"].iloc[0]
            modelo = g["modelo"].iloc[0]

            cap_bus = to_int(g["cap_whats_business"].iloc[0]) or 0
            cap_norm = to_int(g["cap_whats_normal"].iloc[0]) or 0
            capacidade_total = cap_bus + cap_norm

            # Cria slots vazios
            slots = {i: None for i in range(1, capacidade_total + 1)}

            # Chips vinculados
            for _, r in g.iterrows():
                sk_chip = to_int(r.get("sk_chip"))
                slot = to_int(r.get("slot_whatsapp"))

                if not sk_chip or not slot or slot not in slots:
                    continue

                tipo = "BUSINESS" if slot <= cap_bus else "NORMAL"

                slots[slot] = {
                    "sk_chip": sk_chip,
                    "numero": r.get("numero"),
                    "operadora": r.get("operadora"),
                    "tipo_whatsapp": tipo,
                }

            aparelhos.append({
                "sk_aparelho": sk_aparelho,
                "marca": marca,
                "modelo": modelo,
                "capacidade_total": capacidade_total,
                "cap_whats_business": cap_bus,
                "cap_whats_normal": cap_norm,
                "slots": [
                    {"slot": s, "chip": slots[s]}
                    for s in range(1, capacidade_total + 1)
                ],
            })

        # DEBUG (pode remover depois)
        print(f"✅ CHIPS LIVRES CARREGADOS: {len(chips_livres)}")

        return render_template(
            "relacionamentos.html",
            aparelhos=aparelhos,
            chips_livres=chips_livres
        )

    except Exception as e:
        print("🚨 ERRO AO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# VINCULAR CHIP
# ============================================================
@relacionamentos_bp.route("/relacionamentos/vincular", methods=["POST"])
def relacionamentos_vincular():
    try:
        data = request.get_json(force=True) or {}

        sk_chip = to_int(data.get("sk_chip"))
        sk_aparelho = to_int(data.get("sk_aparelho"))
        slot = to_int(data.get("slot"))

        if not sk_chip or not sk_aparelho or not slot:
            return jsonify({"ok": False, "error": "Dados inválidos"}), 400

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
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
        print("🚨 ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# DESVINCULAR CHIP
# ============================================================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True) or {}
        sk_chip = to_int(data.get("sk_chip"))

        if not sk_chip:
            return jsonify({"ok": False, "error": "sk_chip inválido"}), 400

        sql = f"""
        UPDATE `{bq.project}.marts.dim_chip`
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
        print("🚨 ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
