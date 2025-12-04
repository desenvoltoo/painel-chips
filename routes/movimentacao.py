# routes/movimentacao.py
# ---------------------------------------------
# Gestão de Movimentações de Chips/Aparelhos
# ---------------------------------------------

from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

# Nome correto e esperado pelo app.py
movimentacao_bp = Blueprint("movimentacao", __name__)

bq = BigQueryClient()


# =======================================================
# TELA DE MOVIMENTAÇÃO (GET)
# =======================================================
@movimentacao_bp.route("/movimentacao", methods=["GET"])
def movimentacao_home():

    # Chips completos
    chips = sanitize_df(bq.get_view("vw_chips_painel")).to_dict(orient="records")

    # Aparelhos completos
    aparelhos = sanitize_df(bq.get_view("vw_aparelhos")).to_dict(orient="records")

    return render_template(
        "movimentacao.html",
        chips=chips,
        aparelhos=aparelhos
    )


# =======================================================
# ENDPOINT AJAX — HISTÓRICO DO CHIP
# =======================================================
@movimentacao_bp.route("/movimentacao/historico/<int:sk_chip>", methods=["GET"])
def historico_chip(sk_chip):

    query = f"""
        SELECT *
        FROM `painel-universidade.marts.vw_chip_historico_completo`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_movimento DESC
    """

    try:
        df = bq._run(query)
        eventos = sanitize_df(df).to_dict(orient="records")
        return jsonify(eventos)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


# =======================================================
# REGISTRAR MOVIMENTO (POST)
# =======================================================
@movimentacao_bp.route("/movimentacao", methods=["POST"])
def movimentacao_add():

    data = request.form.to_dict()

    sk_chip = int(data.get("sk_chip"))
    sk_aparelho = int(data.get("sk_aparelho")) if data.get("sk_aparelho") else None
    tipo = data.get("tipo_movimento")
    origem = data.get("origem", "Painel")
    observacao = data.get("observacao", "")

    try:
        ok = bq.registrar_movimento_chip(
            sk_chip=sk_chip,
            sk_aparelho=sk_aparelho,
            tipo=tipo,
            origem=origem,
            observacao=observacao
        )
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

    return redirect("/movimentacao")
