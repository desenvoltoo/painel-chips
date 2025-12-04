from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

mov_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()


# ============================
# 📌 PÁGINA PRINCIPAL
# ============================
@mov_bp.route("/movimentacao")
def movimentacao_home():
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))

    return render_template(
        "movimentacao.html",
        chips=chips_df.to_dict(orient="records")
    )


# ============================
# 📌 API – AUTOCOMPLETE
# ============================
@mov_bp.route("/movimentacao/buscar")
def buscar_chip():
    termo = request.args.get("q", "").lower()

    df = sanitize_df(bq.get_view("vw_chips_painel"))

    filtrado = df[df["numero"].str.contains(termo, case=False, na=False)]

    return jsonify(filtrado[["sk_chip", "numero", "operadora"]].to_dict(orient="records"))


# ============================
# 📌 API – HISTÓRICO
# ============================
@mov_bp.route("/movimentacao/historico/<sk_chip>")
def historico_chip(sk_chip):

    sql = f"""
        SELECT *
        FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """

    df = bq._run(sql)
    df = sanitize_df(df)

    return jsonify(df.to_dict(orient="records"))
