from flask import Blueprint, jsonify, request, render_template
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

mov_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()

# ============================================================
# 📌 Página da Movimentação
# ============================================================
@mov_bp.route("/movimentacao")
def pagina_movimentacao():
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
    return render_template("movimentacao.html", chips=chips_df.to_dict(orient="records"))


# ============================================================
# 🔎 AUTOCOMPLETE — Buscar chip por número
# ============================================================
@mov_bp.route("/movimentacao/buscar")
def buscar_chip():
    termo = request.args.get("numero", "").strip()

    if not termo:
        return jsonify([])

    sql = f"""
        SELECT sk_chip, numero, operadora
        FROM `{bq.project}.{bq.dataset}.vw_chips_painel`
        WHERE numero LIKE '%{termo}%'
        ORDER BY numero
        LIMIT 10
    """

    df = bq._run(sql)
    return jsonify(df.to_dict(orient="records"))


# ============================================================
# 📜 TIMELINE COMPLETA DO CHIP
# ============================================================
@mov_bp.route("/movimentacao/timeline/<int:sk_chip>")
def timeline_chip(sk_chip):
    sql = f"""
        SELECT *
        FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """
    df = bq._run(sql)
    return jsonify(df.to_dict(orient="records"))
