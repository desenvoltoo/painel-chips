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

    form = request.form

    sk_chip = form.get("sk_chip")
    valor = form.get("valor")
    data = form.get("data_recarga")

    # Atualiza valores na tabela dim_chip
    sql = f"""
    UPDATE `{bq.PROJECT}.{bq.DATASET}.dim_chip`
    SET 
        ultima_recarga_valor = {valor},
        ultima_recarga_data = DATE('{data}'),
        update_at = CURRENT_TIMESTAMP()
    WHERE sk_chip = {sk_chip}
    """

    bq._run(sql)

    return redirect("/dashboard")
