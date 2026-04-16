from flask import Blueprint, jsonify, render_template, request

from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

recargas_bp = Blueprint("recargas", __name__)
bq = BigQueryClient()


@recargas_bp.route("/recargas")
def recargas_page():
    return render_template("recargas.html")


@recargas_bp.route("/api/chips/listar")
def listar_chips():
    df = sanitize_df(bq.get_view("vw_chips_painel"))
    cols = [c for c in ["sk_chip", "numero", "operadora"] if c in df.columns]
    if not cols:
        return jsonify([])
    return jsonify(df[cols].drop_duplicates().to_dict(orient="records"))


@recargas_bp.route("/api/recargas/listar")
def listar_recargas():
    sql = f"""
        SELECT
            sk_chip,
            CAST(NULL AS INT64) AS id_recarga,
            numero,
            operadora,
            ultima_recarga_valor AS valor,
            CAST(ultima_recarga_data AS STRING) AS data,
            CAST(NULL AS STRING) AS obs
        FROM `{bq.project}.{bq.dataset}.vw_chips_painel`
        WHERE ultima_recarga_data IS NOT NULL
        ORDER BY ultima_recarga_data DESC
    """
    df = sanitize_df(bq.run_df(sql))
    return jsonify(df.to_dict(orient="records"))


@recargas_bp.route("/api/recargas/salvar", methods=["POST"])
def salvar_recarga():
    payload = request.json or {}
    sk_chip = payload.get("id_chip")
    valor = payload.get("valor")
    obs = payload.get("obs") or "Recarga via painel"

    if not sk_chip or valor in [None, ""]:
        return jsonify({"success": False, "error": "Chip e valor são obrigatórios"}), 400

    bq.run(f"""
        CALL `{bq.project}.{bq.dataset}.sp_registrar_recarga_chip`(
            {int(sk_chip)},
            {float(valor)},
            'Painel',
            '{str(obs).replace("'", "''")}'
        )
    """)
    return jsonify({"success": True})
