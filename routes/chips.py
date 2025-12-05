# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

# ============================================================
# HELPERS
# ============================================================

def q_str(v):
    """Strings → 'valor' ou NULL"""
    return f"'{v}'" if v and v != "NULL" else "NULL"


def q_date(v):
    """Datas → DATE('yyyy-mm-dd') ou NULL"""
    if not v:
        return "NULL"
    return f"DATE('{v}')"


def q_num(v):
    """Números → número ou NULL"""
    return str(v) if v not in (None, "", "NULL") else "NULL"


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
# CADASTRAR NOVO CHIP — AGORA GERANDO SK_AUTOMÁTICO
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    dados = request.form.to_dict()

    def q(v):
        return f"'{v}'" if v else "NULL"

    # 1️⃣ Buscar o próximo SK
    get_sk = """
        SELECT COALESCE(MAX(sk_chip), 0) + 1 AS next_sk
        FROM `painel-universidade.marts.dim_chip`
    """
    df_sk = bq._run(get_sk)
    next_sk = int(df_sk.iloc[0]["next_sk"])

    # 2️⃣ Executar o INSERT com o novo SK
    query = f"""
        INSERT INTO `painel-universidade.marts.dim_chip`
        (sk_chip, id_chip, numero, operadora, operador, status, plano, dt_inicio,
         ultima_recarga_valor, ultima_recarga_data, total_gasto, 
         sk_aparelho_atual, observacao, ativo, created_at, updated_at)
        VALUES (
            {next_sk},
            {q(dados.get("id_chip"))},
            {q(dados.get("numero"))},
            {q(dados.get("operadora"))},
            {q(dados.get("operador"))},
            {q(dados.get("status"))},
            {q(dados.get("plano"))},
            {q(dados.get("dt_inicio"))},
            {dados.get("ultima_recarga_valor") or "NULL"},
            {q(dados.get("ultima_recarga_data"))},
            {dados.get("total_gasto") or "NULL"},
            {dados.get("sk_aparelho_atual") or "NULL"},
            {q(dados.get("observacao"))},
            TRUE,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        )
    """

    bq.execute_query(query)

    return "<script>alert('Chip cadastrado com sucesso!'); window.location.href='/chips';</script>"


# ============================================================
# BUSCAR CHIP POR SK (EDIÇÃO)
# ============================================================

@chips_bp.route("/chips/sk/<sk_chip>")
def chips_get_by_sk(sk_chip):
    query = f"""
        SELECT *
        FROM `painel-universidade.marts.vw_chips_painel`
        WHERE sk_chip = {sk_chip}
        LIMIT 1
    """

    df = bq._run(query)

    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    return jsonify(df.to_dict(orient="records")[0])


# ============================================================
# ATUALIZAR CHIP (SALVAR DO MODAL)
# ============================================================

@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    data = request.json

    def q(v):
        return f"'{v}'" if v else "NULL"

    def qdate(v):
        from utils.bigquery_client import normalize_date
        return normalize_date(v)

    def qnum(v):
        from utils.bigquery_client import normalize_number
        return normalize_number(v)

    query = f"""
        UPDATE `{PROJECT}.{DATASET}.dim_chip`
        SET
            numero = {q(data.get("numero"))},
            operadora = {q(data.get("operadora"))},
            operador = {q(data.get("operador"))},
            plano = {q(data.get("plano"))},
            status = {q(data.get("status"))},
            observacao = {q(data.get("observacao"))},
            dt_inicio = {qdate(data.get("dt_inicio"))},
            ultima_recarga_valor = {qnum(data.get("ultima_recarga_valor"))},
            ultima_recarga_data = {qdate(data.get("ultima_recarga_data"))},
            total_gasto = {qnum(data.get("total_gasto"))},
            sk_aparelho_atual = {data.get("sk_aparelho_atual") or "NULL"},
            updated_at = CURRENT_TIMESTAMP()
        WHERE sk_chip = {data.get("sk_chip")}
    """

    print("\n🔵 UPDATE VIA JSON:\n", query)

    # AQUI ESTAVA O ERRO — EXECUTA COM _run()
    bq._run(query)

    return jsonify({"success": True})

