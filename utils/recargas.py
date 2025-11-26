# utils/recargas.py

from flask import Blueprint, request, redirect
from utils.bigquery_client import BigQueryClient

recargas_bp = Blueprint("recargas", __name__)
bq = BigQueryClient()


# =======================================================
# REGISTRAR RECARGA
# =======================================================
@recargas_bp.route("/recargas/add", methods=["POST"])
def registrar_recarga():
    """Registra manualmente uma recarga para um chip."""

    sk_chip = request.form.get("sk_chip")
    valor = request.form.get("valor")
    data = request.form.get("data_recarga")

    if not sk_chip or not valor or not data:
        return "Dados inválidos", 400

    sql = f"""
        UPDATE `{bq.project}.{bq.dataset}.dim_chip`
        SET 
            ultima_recarga_valor = {valor},
            ultima_recarga_data = DATE('{data}'),
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {sk_chip}
    """

    bq.execute(sql)

    return redirect("/dashboard")
