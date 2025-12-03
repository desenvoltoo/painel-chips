# routes/movimentacao.py
# ---------------------------------------------
# Gestão de Movimentações de Chips/Aparelhos
# ---------------------------------------------

from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

mov_bp = Blueprint("movimentacao", __name__)

bq = BigQueryClient()


# =======================================================
# TELA DE MOVIMENTAÇÕES (GET)
# =======================================================
@mov_bp.route("/movimentacao", methods=["GET"])
def movimentacao_home():

    # Chips completos
    chips = sanitize_df(bq.get_view("vw_chips_painel")).to_dict(orient="records")

    # Aparelhos completos
    aparelhos = sanitize_df(bq.get_view("vw_aparelhos")).to_dict(orient="records")

    # Histórico de movimentações
    eventos = sanitize_df(bq.get_view("vw_chip_historico")).to_dict(orient="records")

    return render_template(
        "movimentacao.html",
        chips=chips,
        aparelhos=aparelhos,
        eventos=eventos
    )


# =======================================================
# REGISTRAR MOVIMENTAÇÃO (POST)
# =======================================================
@mov_bp.route("/movimentacao", methods=["POST"])
def movimentacao_add():

    data = request.form.to_dict()

    sk_chip = int(data.get("sk_chip"))
    sk_aparelho = int(data.get("sk_aparelho")) if data.get("sk_aparelho") else None
    tipo = data.get("tipo_movimento")
    origem = data.get("origem", "Painel")
    observacao = data.get("observacao", "")

    # Chamada da SP que registra o movimento
    ok = bq.registrar_movimento_chip(
        sk_chip=sk_chip,
        sk_aparelho=sk_aparelho,
        tipo=tipo,
        origem=origem,
        observacao=observacao
    )

    if ok:
        return redirect("/movimentacao")
    else:
        return jsonify({"erro": "Falha ao registrar movimentação"}), 500
