# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

rel_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()

# ===============================================================
# 📌 PÁGINA PRINCIPAL
# ===============================================================
@rel_bp.route("/relacionamentos")
def pagina_relacionamentos():
    try:
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))
        rel_df = sanitize_df(bq.get_view("vw_relacionamentos_whatsapp"))
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

        # agrupar chips por aparelho
        aparelhos = {}
        for a in aparelhos_df.to_dict(orient="records"):
            a["slots"] = [
                {"slot": i + 1, "chip": None}
                for i in range(a["capacidade_whatsapp"])
            ]
            aparelhos[a["sk_aparelho"]] = a

        # preencher slots ocupados
        for r in rel_df.to_dict(orient="records"):
            aparelho = aparelhos.get(r["sk_aparelho"])
            if aparelho:
                aparelho["slots"][r["slot"] - 1]["chip"] = {
                    "sk_chip": r["sk_chip"],
                    "numero": r["numero"],
                    "operadora": r["operadora"]
                }

        # chips não atribuídos
        chips_sem_slot = []
        for c in chips_df.to_dict(orient="records"):
            encontrado = False
            for a in aparelhos.values():
                for slot in a["slots"]:
                    if slot["chip"] and slot["chip"]["sk_chip"] == c["sk_chip"]:
                        encontrado = True
                        break
                if encontrado:
                    break
            if not encontrado:
                chips_sem_slot.append(c)

        # adicionar lista de livres por aparelho
        for a in aparelhos.values():
            a["chips_sem_slot"] = chips_sem_slot

        return render_template("relacionamentos.html", aparelhos=list(aparelhos.values()))

    except Exception as e:
        print("ERRO CARREGAR RELACIONAMENTOS:", e)
        return "Erro", 500


# ===============================================================
# ➕ AUTO-SAVE — Vincular chip ao slot
# ===============================================================
@rel_bp.route("/relacionamentos/vincular", methods=["POST"])
def vincular_chip():
    try:
        data = request.json
        sk_chip = data["sk_chip"]
        sk_aparelho = data["sk_aparelho"]
        slot = data["slot"]

        sql = f"""
            INSERT INTO `painel-universidade.marts.relacionamentos_whatsapp`
            (sk_chip, sk_aparelho, slot, updated_at)
            VALUES ({sk_chip}, {sk_aparelho}, {slot}, CURRENT_TIMESTAMP())
            ON CONFLICT(sk_aparelho, slot) DO UPDATE SET sk_chip={sk_chip}, updated_at=CURRENT_TIMESTAMP();
        """

        bq.execute(sql)
        return jsonify({"status": "ok"})

    except Exception as e:
        print("ERRO VINCULAR:", e)
        return jsonify({"erro": str(e)}), 500


# ===============================================================
# ❌ AUTO-SAVE — Desvincular chip do slot
# ===============================================================
@rel_bp.route("/relacionamentos/desvincular", methods=["POST"])
def desvincular_chip():
    try:
        data = request.json
        sk_aparelho = data["sk_aparelho"]
        slot = data["slot"]

        sql = f"""
            DELETE FROM `painel-universidade.marts.relacionamentos_whatsapp`
            WHERE sk_aparelho={sk_aparelho} AND slot={slot}
        """

        bq.execute(sql)
        return jsonify({"status": "ok"})

    except Exception as e:
        print("ERRO DESVINCULAR:", e)
        return jsonify({"erro": str(e)}), 500
