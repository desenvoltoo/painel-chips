# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# ============================================================
# LISTAR CHIPS
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
    aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )


# ============================================================
# CADASTRAR NOVO CHIP
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    dados = request.form.to_dict()

    query = f"""
        INSERT INTO `painel-universidade.marts.dim_chip`
        (id_chip, numero, operadora, operador, status, plano, dt_inicio,
         ultima_recarga_valor, ultima_recarga_data, total_gasto, 
         sk_aparelho_atual, observacao)
        VALUES (
            '{dados.get("id_chip")}',
            '{dados.get("numero")}',
            '{dados.get("operadora")}',
            '{dados.get("operador")}',
            '{dados.get("status")}',
            '{dados.get("plano")}',
            {'NULL' if not dados.get("dt_inicio") else f"'{dados.get('dt_inicio')}'"},
            {dados.get("ultima_recarga_valor") or "NULL"},
            {'NULL' if not dados.get("ultima_recarga_data") else f"'{dados.get('ultima_recarga_data')}'"},
            {dados.get("total_gasto") or "NULL"},
            {dados.get("sk_aparelho_atual") or "NULL"},
            '{dados.get("observacao")}'
        )
    """

    bq.execute_query(query)
    return "<script>alert('Chip cadastrado com sucesso!'); window.location.href='/chips';</script>"


# ============================================================
# BUSCAR CHIP PARA EDIÇÃO — BUSCA PELO SK_CHIP (CORRETO)
# ============================================================
@chips_bp.route("/chips/sk/<sk_chip>")
def chips_get_by_sk(sk_chip):

    query = f"""
        SELECT *
        FROM `painel-universidade.marts.vw_chips_painel`
        WHERE sk_chip = {sk_chip}
        LIMIT 1
    """

    df = bq.execute_query(query)

    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    return jsonify(df.to_dict(orient="records")[0])


# ============================================================
# ATUALIZAR CHIP (SALVAR DO MODAL)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    data = request.json

    query = f"""
        UPDATE `painel-universidade.marts.dim_chip`
        SET
            numero = '{data.get("numero")}',
            operadora = '{data.get("operadora")}',
            operador = '{data.get("operador")}',
            status = '{data.get("status")}',
            plano = '{data.get("plano")}',
            dt_inicio = {'NULL' if not data.get("dt_inicio") else f"'{data.get('dt_inicio')}'"},
            ultima_recarga_data = {'NULL' if not data.get("ultima_recarga_data") else f"'{data.get('ultima_recarga_data')}'"},
            ultima_recarga_valor = {data.get("ultima_recarga_valor") or "NULL"},
            total_gasto = {data.get("total_gasto") or "NULL"},
            sk_aparelho_atual = {data.get("sk_aparelho_atual") or "NULL"},
            observacao = '{data.get("observacao")}'
        WHERE sk_chip = {data.get("sk_chip")}
    """

    bq.execute_query(query)

    return jsonify({"success": True})
