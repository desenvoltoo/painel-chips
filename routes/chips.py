# routes/chips.py

from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# =====================================================================
# LISTA DE CHIPS
# =====================================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = bq.get_view("vw_chips_painel")
    chips_df = sanitize_df(chips_df)

    aparelhos_df = bq.get_view("vw_aparelhos")
    aparelhos_df = sanitize_df(aparelhos_df)

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )


# =====================================================================
# FORM HTML DE EDIÇÃO
# =====================================================================
@chips_bp.route("/chips/edit/<id_chip>")
def chips_edit(id_chip):
    chips_df = bq.get_view("vw_chips_painel")
    chips_df = sanitize_df(chips_df)

    chip = chips_df[chips_df["id_chip"] == id_chip]

    if chip.empty:
        return "Chip não encontrado", 404

    chip = chip.to_dict(orient="records")[0]

    aparelhos_df = bq.get_view("vw_aparelhos")
    aparelhos_df = sanitize_df(aparelhos_df).to_dict(orient="records")

    return render_template(
        "chips_edit.html",
        chip=chip,
        aparelhos=aparelhos_df
    )


# =====================================================================
# SALVAR ALTERAÇÕES DO CHIP
# =====================================================================
@chips_bp.route("/chips/update", methods=["POST"])
def chips_update():
    try:
        bq.upsert_chip(request.form)
        return redirect("/chips")

    except Exception as e:
        print("Erro ao atualizar chip:", e)
        return "Erro ao atualizar chip", 500


# =====================================================================
# API - GET CHIP (JSON)
# =====================================================================
@chips_bp.route("/chips/<id_chip>")
def get_chip(id_chip):
    df = bq.get_view("vw_chips_painel")
    df = df[df["id_chip"] == id_chip]

    if df.empty:
        return jsonify({"erro": "Chip não encontrado"}), 404

    return jsonify(df.to_dict(orient="records")[0])


# =====================================================================
# REGISTRAR MOVIMENTO
# =====================================================================
@chips_bp.route("/chips/movimento", methods=["POST"])
def chips_movimento():
    try:
        dados = request.json

        ok = bq.registrar_movimento_chip(
            sk_chip=dados.get("sk_chip"),
            sk_aparelho=dados.get("sk_aparelho"),
            tipo=dados.get("tipo"),
            origem=dados.get("origem", "Painel"),
            observacao=dados.get("observacao", "")
        )

        return jsonify({"success": ok})

    except Exception as e:
        print("Erro movimento chip:", e)
        return jsonify({"success": False, "erro": str(e)}), 500
