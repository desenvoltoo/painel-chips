# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

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
        return int(float(v))
    except:
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
        # -------------------------------------------
        # 1) APARELHOS + CHIPS VINCULADOS (VIEW)
        # -------------------------------------------
        df = sanitize_df(bq.get_view("vw_relacionamentos_whatsapp"))

        # -------------------------------------------
        # 2) CHIPS LIVRES (DIM)
        # -------------------------------------------
        chips_livres_df = bq.run_df(f"""
            SELECT
                sk_chip,
                numero,
                operadora,
                tipo_whatsapp
            FROM `{bq.project}.{bq.dataset}.dim_chip`
            WHERE ativo = TRUE
              AND sk_aparelho_atual IS NULL
            ORDER BY numero
        """)

        chips_livres = []
        if not chips_livres_df.empty:
            for _, r in chips_livres_df.iterrows():
                chips_livres.append({
                    "sk_chip": to_int(r.get("sk_chip")),
                    "numero": r.get("numero"),
                    "operadora": r.get("operadora"),
                    "tipo_whatsapp": r.get("tipo_whatsapp") or "A DEFINIR",
                })

        # -------------------------------------------
        # Se não houver aparelhos
        # -------------------------------------------
        if df.empty:
            return render_template(
                "relacionamentos.html",
                aparelhos=[],
                chips_livres=chips_livres
            )

        # -------------------------------------------
        # 3) AGRUPAR APARELHOS E SLOTS
        # -------------------------------------------
        aparelhos = []

        for sk_aparelho, g in df.groupby("sk_aparelho"):
            sk_aparelho = to_int(sk_aparelho)
            if not sk_aparelho:
                continue

            marca = g["marca"].iloc[0]
            modelo = g["modelo"].iloc[0]

            cap_bus = to_int(g["cap_whats_business"].iloc[0]) if "cap_whats_business" in g.columns else 0
            cap_norm = to_int(g["cap_whats_normal"].iloc[0]) if "cap_whats_normal" in g.columns else 0
            capacidade_total = (cap_bus or 0) + (cap_norm or 0)

            # cria slots vazios
            slots = {i: None for i in range(1, capacidade_total + 1)}

            # chips vinculados
            for _, r in g.iterrows():
                sk_chip = to_int(r.get("sk_chip"))
                slot = to_int(r.get("slot_whatsapp"))

                if not sk_chip or not slot or slot not in slots:
                    continue

                tipo = r.get("tipo_whatsapp")
                if is_null(tipo):
                    tipo = "BUSINESS" if (cap_bus and slot <= cap_bus) else "NORMAL"

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
                "cap_whats_business": cap_bus or 0,
                "cap_whats_normal": cap_norm or 0,
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
        print("🚨 ERRO AO CARREGAR RELACIONAMENTOS:", e)
        return "Erro ao carregar relacionamentos", 500


# ============================================================
# VINCULAR CHIP (SP CORRETA)
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

        # -------------------------------------------
        # 1) VINCULA CHIP + SLOT
        # -------------------------------------------
        bq.run(f"""
            CALL `{bq.project}.{bq.dataset}.sp_vincular_aparelho_chip`(
                {sk_chip},
                {sk_aparelho},
                {slot},
                'Painel',
                'Vínculo WhatsApp'
            )
        """)

        # -------------------------------------------
        # 2) REGISTRA MOVIMENTAÇÃO
        # -------------------------------------------
        bq.run(f"""
            CALL `{bq.project}.{bq.dataset}.sp_registrar_movimento_chip`(
                {sk_chip},
                'VINCULO',
                'Painel',
                'Vinculado ao aparelho {sk_aparelho} no slot {slot}'
            )
        """)

        return jsonify({"ok": True})

    except Exception as e:
        print("🚨 ERRO VINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# DESVINCULAR CHIP (SP CORRETA)
# ============================================================
@relacionamentos_bp.route("/relacionamentos/desvincular", methods=["POST"])
def relacionamentos_desvincular():
    try:
        data = request.get_json(force=True) or {}
        sk_chip = to_int(data.get("sk_chip"))

        if not sk_chip:
            return jsonify({"ok": False, "error": "sk_chip inválido"}), 400

        # -------------------------------------------
        # 1) DESVINCULA CHIP + SLOT
        # -------------------------------------------
        bq.run(f"""
            CALL `{bq.project}.{bq.dataset}.sp_desvincular_aparelho_chip`(
                {sk_chip},
                'Painel',
                'Desvinculação de aparelho'
            )
        """)

        # -------------------------------------------
        # 2) REGISTRA MOVIMENTAÇÃO
        # -------------------------------------------
        bq.run(f"""
            CALL `{bq.project}.{bq.dataset}.sp_registrar_movimento_chip`(
                {sk_chip},
                'DESVINCULO',
                'Painel',
                'Chip desvinculado do aparelho'
            )
        """)

        return jsonify({"ok": True})

    except Exception as e:
        print("🚨 ERRO DESVINCULAR:", e)
        return jsonify({"ok": False, "error": str(e)}), 500
