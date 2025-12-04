from flask import Blueprint, render_template, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

movimentacao_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()

@movimentacao_bp.route("/movimentacao", methods=["GET"])
def movimentacao_home():
    chips = sanitize_df(bq.get_view("vw_chips_painel")).to_dict(orient="records")
    return render_template("movimentacao.html", chips=chips)

@movimentacao_bp.route("/movimentacao/historico/<int:sk_chip>", methods=["GET"])
def movimentacao_historico(sk_chip):

    sql = f"""
        SELECT 
            sk_chip,
            data_evento,
            tipo_evento,
            campo,
            valor_antigo,
            valor_novo,
            origem,
            observacao
        FROM `{bq.project}.{bq.dataset}.vw_chip_historico_completo`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """

    df = bq._run(sql)
    df = sanitize_df(df)

    return jsonify(df.to_dict(orient="records"))
