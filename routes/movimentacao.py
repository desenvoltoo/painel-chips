# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

movimentacao_bp = Blueprint("movimentacao", __name__)
bq = BigQueryClient()

# ===========================
# TELA PRINCIPAL
# ===========================
@movimentacao_bp.route("/movimentacao", methods=["GET"])
def movimentacao_home():
    chips = sanitize_df(bq.get_view("vw_chips_painel")).to_dict(orient="records")
    aparelhos = sanitize_df(bq.get_view("vw_aparelhos")).to_dict(orient="records")

    return render_template(
        "movimentacao.html",
        chips=chips,
        aparelhos=aparelhos
    )

# ===========================
# ROTA AJAX HISTÓRICO
# ===========================
@movimentacao_bp.route("/movimentacao/historico/<int:sk_chip>", methods=["GET"])
def movimentacao_historico(sk_chip):

    sql = f"""
        SELECT 
            data_movimento,
            tipo_movimento,
            origem,
            observacao,
            marca_aparelho,
            modelo_aparelho
        FROM `{bq.project}.{bq.dataset}.vw_chip_historico_completo`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_movimento DESC
    """

    df = bq._run(sql)
    df = sanitize_df(df)

    return jsonify(df.to_dict(orient="records"))

# ===========================
# REGISTRAR MOVIMENTO
# ===========================
@movimentacao_bp.route("/movimentacao", methods=["POST"])
def movimentacao_add():
    data = request.form.to_dict()

    sk_chip = int(data.get("sk_chip"))
    sk_aparelho = int(data.get("sk_aparelho")) if data.get("sk_aparelho") else None
    tipo = data.get("tipo_movimento")
    origem = data.get("origem", "Painel")
    observacao = data.get("observacao", "")

    ok = bq.registrar_movimento_chip(
        sk_chip=sk_chip,
        sk_aparelho=sk_aparelho,
        tipo=tipo,
        origem=origem,
        observacao=observacao
    )

    return jsonify({"status": "ok" if ok else "erro"})
