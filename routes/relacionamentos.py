# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()

PROJECT = bq.project
DATASET = bq.dataset


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
        return int(float(v))
    except Exception:
        return None


def is_null(v):
    return (
        v is None
        or str(v).strip() == ""
        or str(v).strip().lower() in ["none", "null", "nan"]
    )


# ============================================================
# HOME — RELACIONAMENTOS
# ============================================================
@relacionamentos_bp.route("/relacionamentos")
def relacionamentos_home():
    try:
        # ====================================================
        # 1) VIEW RELACIONAMENTOS
        # ====================================================
        df = sanitize_df(
            bq.run_df(f"""
                SELECT *
                FROM `{PROJECT}.{DATASET}.vw_relacionamentos_whatsapp`
            """)
        )

        # ====================================================
        # 2) CHIPS LIVRES
        # ====================================================
        chips_livres_df = bq.run_df(f"""
            SELECT
                sk_chip,
                numero,
                operadora,
                tipo_whatsapp
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE ativo = TRUE
              AND sk_aparelho_atual IS NULL
            ORDER BY numero
        """)

        chips_livres = []
        if not chips_livres_df.empty:
            for _, r in chips_livres_df.iterrows():
                chips_livres.append({
                    "sk_chip": to_int(r["sk_chip"]),
                    "numero": r["numero"],
                    "operadora": r["operadora"],
                    "tipo_whatsapp": r.get("tipo_whatsapp") or "A DEFINIR",
                })

        # ====================================================
        # SE NÃO EXISTIR APARELHO
        # ====================================================
        if df.empty:
            return render_template(
                "relacionamentos.html",
                aparelhos=[],
                chips_livres=chips_livres
            )

        # ====================================================
        # 3) AGRUPAR POR APARELHO
        # ====================================================
        aparelhos = []

        for sk_aparelho, g in df.groupby("sk_aparelho"):
            sk_aparelho = to_int(sk_aparelho)
            if not sk_aparelho:
                continue

            marca = g["marca"].iloc[0]
            modelo = g["modelo"].iloc[0]

            cap_bus = (
                to_int(g["cap_whats_business"].iloc[0])
                if "cap_whats_business" in g.columns
                else 0
            )

            cap_norm = (
                to_int(g["cap_whats_normal"].iloc[0])
                if "cap_whats_normal" in g.columns
                else 0
            )

            capacidade_total = cap_bus + cap_norm

            # ------------------------------------------------
            # CRIA TODOS OS SLOTS VAZIOS
            # ------------------------------------------------
            slots = {i: None for i in range(1, capacidade_total + 1)}

            # ------------------------------------------------
            # PREENCHE SLOTS OCUPADOS
            # ------------------------------------------------
            for _, r in g.iterrows():
                sk_chip = to_int(r.get("sk_chip"))
                slot = to_int(r.get("slot_whatsapp"))

                if not sk_chip or not slot or slot not in slots:
                    continue

                tipo = r.get("tipo_whatsapp")
                if is_null(tipo):
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

        return render_template(
            "relacionamentos.html",
            aparelhos=aparelhos,
            chips_livres=chips_livres
        )

    except Exception as e:
        print("🚨 ERRO RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# VINCULAR CHIP (COM SLOT — SP FINAL)
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

        bq.run(f"""
            CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                {sk_chip},
                {sk_aparelho},
                {slot},
                'Painel',
                'Vínculo WhatsApp'
            )
        """)

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

        bq.run(f"""
            CALL `{PROJECT}.{DATASET}.sp_desvincular_aparelho_chip`(
                {sk_chip},
                'Painel',
                'Desvinculação de aparelho'
            )
        """)

        return jsonify({"ok": True})

    except Exception as e:
        print("🚨 ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
