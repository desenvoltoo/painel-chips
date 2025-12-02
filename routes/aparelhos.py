# routes/aparelhos.py
# ---------------------------------------------
# Rotas da gestão de Aparelhos (CRUD)
# ---------------------------------------------

from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df  # usa a sanitização padrão

# Nome do blueprint (é isso que vamos importar no app.py)
aparelhos_bp = Blueprint("aparelhos", __name__)
bq = BigQueryClient()


# =======================================================
# LISTAGEM DE APARELHOS
# =======================================================
@aparelhos_bp.route("/aparelhos")
def aparelhos_list():
    try:
        df = bq.get_view("vw_aparelhos")
        df = sanitize_df(df)

        return render_template(
            "aparelhos.html",
            aparelhos=df.to_dict(orient="records")
        )

    except Exception as e:
        print("Erro ao carregar aparelhos:", e)
        return "Erro ao carregar aparelhos", 500


# =======================================================
# INSERIR / EDITAR (UPSERT)
# =======================================================
@aparelhos_bp.route("/aparelhos/add", methods=["POST"])
def aparelhos_add():
    try:
        dados = {
            "id_aparelho": request.form.get("id_aparelho"),
            "modelo": request.form.get("modelo"),
            "marca": request.form.get("marca"),
            "imei": request.form.get("imei"),
            "status": request.form.get("status"),
        }

        # upsert no BigQuery
        bq.upsert_aparelho(dados)

        # volta para a tela de aparelhos
        return redirect("/aparelhos")

    except Exception as e:
        print("Erro ao salvar aparelho:", e)
        return "Erro ao salvar aparelho", 500
