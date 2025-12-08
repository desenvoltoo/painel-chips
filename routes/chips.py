# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient, normalize_date, normalize_number
from utils.sanitizer import sanitize_df
import os

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")


# ============================================================
# HELPERS PADRONIZADOS
# ============================================================

def q_str(v):
    return f"'{v}'" if v else "NULL"

def q_date(v):
    return normalize_date(v)

def q_num(v):
    return normalize_number(v)


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
        aparelhos=aparelhos_df.to_dict(orient="records"),
    )


# ============================================================
# CADASTRAR CHIP
# ============================================================

@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    dados = request.form.to_dict()

    get_sk = f"""
        SELECT COALESCE(MAX(sk_chip), 0) + 1 AS next_sk
        FROM `{PROJECT}.{DATASET}.dim_chip`
    """
    next_sk = int(bq._run(get_sk).iloc[0]["next_sk"])

    query = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_chip`
        (sk_chip, id_chip, numero, operadora, operador, status, plano, dt_inicio,
         ultima_recarga_valor, ultima_recarga_data, total_gasto, 
         sk_aparelho_atual, observacao, ativo, created_at, updated_at)
        VALUES (
            {next_sk},
            {q_str(dados.get("id_chip"))},
            {q_str(dados.get("numero"))},
            {q_str(dados.get("operadora"))},
            {q_str(dados.get("operador"))},
            {q_str(dados.get("status"))},
            {q_str(dados.get("plano"))},
            {q_date(dados.get("dt_inicio"))},
            {q_num(dados.get("ultima_recarga_valor"))},
            {q_date(dados.get("ultima_recarga_data"))},
            {q_num(dados.get("total_gasto"))},
            {dados.get("sk_aparelho_atual") or "NULL"},
            {q_str(dados.get("observacao"))},
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    """

    bq.execute_query(query)
    return "<script>alert('Chip cadastrado com sucesso!'); window.location.href='/chips';</script>"


# ============================================================
# BUSCAR CHIP POR SK
# ============================================================

@chips_bp.route("/chips/sk/<sk_chip>")
def chips_get_by_sk(sk_chip):
    query = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chips_painel`
        WHERE sk_chip = {sk_chip}
        LIMIT 1
    """

    df = bq._run(query)
    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404
    return jsonify(df.to_dict(orient="records")[0])


# ============================================================
# ATUALIZAR CHIP
# ============================================================

@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    data = request.json

    query = f"""
        UPDATE `{PROJECT}.{DATASET}.dim_chip`
        SET
            numero = {q_str(data.get("numero"))},
            operadora = {q_str(data.get("operadora"))},
            operador = {q_str(data.get("operador"))},
            plano = {q_str(data.get("plano"))},
            status = {q_str(data.get("status"))},
            observacao = {q_str(data.get("observacao"))},

            dt_inicio = {q_date(data.get("dt_inicio"))},
            ultima_recarga_data = {q_date(data.get("ultima_recarga_data"))},

            ultima_recarga_valor = {q_num(data.get("ultima_recarga_valor"))},
            total_gasto = {q_num(data.get("total_gasto"))},

            sk_aparelho_atual = {data.get("sk_aparelho_atual") or "NULL"},
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {data.get("sk_chip")}
    """

    print("\n🔵 UPDATE VIA JSON:\n", query)

    bq._run(query)

    return jsonify({"success": True})
