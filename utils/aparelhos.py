# routes/aparelhos.py
# ---------------------------------------------
# Rotas da gestão de Aparelhos (CRUD)
# ---------------------------------------------

from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from app import sanitize_df  # usa a sanitização correta

bp_aparelhos = Blueprint("aparelhos", __name__)
bq = BigQueryClient()


# =======================================================
# LISTAGEM
# =======================================================
@bp_aparelhos.route("/aparelhos")
def aparelhos():
    try:
        aparelhos_df = bq.get_view("vw_aparelhos")
        aparelhos_df = sanitize_df(aparelhos_df)

        return render_template(
            "aparelhos.html",
            aparelhos=aparelhos_df.to_dict(orient="records")
        )

    except Exception as e:
        print("Erro ao carregar aparelhos:", e)
        return "Erro ao carregar aparelhos", 500


# =======================================================
# UPSERT (INSERT + UPDATE)
# =======================================================
@bp_aparelhos.route("/aparelhos/add", methods=["POST"])
def add_aparelho():
    try:
        dados = {
            "id_aparelho": request.form.get("id_aparelho"),
            "modelo": request.form.get("modelo"),
            "marca": request.form.get("marca"),
            "imei": request.form.get("imei"),
            "status": request.form.get("status"),
        }

        bq.upsert_aparelho(dados)
        return redirect("/aparelhos")

    except Exception as e:
        print("Erro ao salvar aparelho:", e)
        return "Erro ao salvar aparelho", 500
