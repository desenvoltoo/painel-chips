# routes/movimentacao.py
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

movimentacao_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()


@movimentacao_bp.route("/movimentacao")
def movimentacao_home():

    chips = sanitize_df(bq.get_view("vw_chips_painel"))
    aparelhos = sanitize_df(bq.get_view("vw_aparelhos"))

    chips_list = chips.to_dict(orient="records")

    # timeline do primeiro chip por padrão
    first_sk = chips_list[0]["sk_chip"] if chips_list else None
    timeline = []

    if first_sk:
        timeline = sanitize_df(bq._run(f"""
            SELECT *
            FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
            WHERE sk_chip = {first_sk}
            ORDER BY data_evento DESC
        """)).to_dict(orient="records")

    return render_template(
        "movimentacao.html",
        chips=chips_list,
        aparelhos=aparelhos.to_dict(orient="records"),
        timeline=timeline,
        selected_chip=first_sk,
    )
