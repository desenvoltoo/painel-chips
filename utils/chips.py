# utils/chips.py
from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient

bq = BigQueryClient()
chips_bp = Blueprint("chips", __name__)


# =======================================================
# LISTAGEM DE CHIPS
# =======================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = bq.get_chips()
    aparelhos_df = bq.get_aparelhos()

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
    bq.upsert_chip(request.form)
    return redirect("/chips")


# =======================================================
# GET (APENAS UM CHIP — se você quiser depois)
# =======================================================
@chips_bp.route("/chips/<id_chip>")
def get_chip(id_chip):
    df = bq.get_chips()
    df = df[df["id_chip"] == id_chip]

    if len(df) == 0:
        return {"erro": "Chip não encontrado"}, 404

    return df.to_dict(orient="records")[0]
