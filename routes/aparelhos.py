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
        df = sanitize_df(bq.get_view("vw_aparelhos"))

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
            # Identificação
            "id_aparelho": request.form.get("id_aparelho"),
            "modelo": request.form.get("modelo"),
            "marca": request.form.get("marca"),
            "imei": request.form.get("imei"),

            # Status
            "status": request.form.get("status"),
            "ativo": True,

            # Capacidades (defaults seguros)
            "qtd_whatsapp_total": request.form.get("qtd_whatsapp_total") or None,
            "qtd_whatsapp_business": request.form.get("qtd_whatsapp_business") or None,
            "qtd_whatsapp_normal": request.form.get("qtd_whatsapp_normal") or None,

            "capacidade_whatsapp": request.form.get("capacidade_whatsapp") or None,
            "cap_whats_business": request.form.get("cap_whats_business") or None,
            "cap_whats_normal": request.form.get("cap_whats_normal") or None,
        }

        bq.upsert_aparelho(payload)

        return redirect("/aparelhos")

    except Exception as e:
        print("Erro ao salvar aparelho:", e)
        return "Erro ao salvar aparelho", 500
