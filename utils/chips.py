# routes/chips.py
from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from app import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# =======================================================
# LISTAGEM DE CHIPS
# =======================================================
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


# =======================================================
# UPSERT (INSERIR + EDITAR)
# =======================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        bq.upsert_chip(request.form)
        return redirect("/chips")
    except Exception as e:
        print("Erro ao salvar chip:", e)
        return "Erro ao salvar chip", 500


# =======================================================
# GET (RETORNAR APENAS 1 CHIP)
# =======================================================
@chips_bp.route("/chips/<id_chip>")
def get_chip(id_chip):
    df = bq.get_view("vw_chips_painel")
    df = df[df["id_chip"] == id_chip]

    if df.empty:
        return jsonify({"erro": "Chip não encontrado"}), 404

    return jsonify(df.to_dict(orient="records")[0])


# =======================================================
# MOVIMENTAÇÃO DO CHIP
# =======================================================
@chips_bp.route("/chips/movimento", methods=["POST"])
def chips_movimento():
    try:
        dados = request.json

        sk_chip = dados.get("sk_chip")
        sk_aparelho = dados.get("sk_aparelho")  # pode ser None
        tipo = dados.get("tipo")
        origem = dados.get("origem", "Painel")
        observacao = dados.get("observacao", "")

        ok = bq.registrar_movimento_chip(
            sk_chip=sk_chip,
            sk_aparelho=sk_aparelho,
            tipo=tipo,
            origem=origem,
            observacao=observacao
        )

        return jsonify({"success": ok})

    except Exception as e:
        print("Erro movimento chip:", e)
        return jsonify({"success": False, "erro": str(e)}), 500
