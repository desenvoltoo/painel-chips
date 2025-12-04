# routes/aparelhos.py
from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

aparelhos_bp = Blueprint("aparelhos", __name__)
bq = BigQueryClient()

# =======================================================
# LISTAGEM
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
# UPSERT (INSERT + UPDATE)
# =======================================================
@aparelhos_bp.route("/aparelhos/add", methods=["POST"])
def aparelhos_add():
    try:
        payload = {
            "id_aparelho": request.form.get("id_aparelho"),
            "modelo": request.form.get("modelo"),
            "marca": request.form.get("marca"),
            "imei": request.form.get("imei"),
            "status": request.form.get("status"),
        }

        bq.upsert_aparelho(payload)

        return redirect("/aparelhos")

    except Exception as e:
        print("Erro ao salvar aparelho:", e)
        return "Erro ao salvar aparelho", 500
