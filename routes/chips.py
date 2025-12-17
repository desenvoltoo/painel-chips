# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from google.cloud import bigquery
import os

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")


# ============================================================
# 🔧 EXECUTAR SP COM PARÂMETROS
# ============================================================
def call_sp(sql: str, params: list):
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    bq.client.query(sql, job_config=job_config).result()


# ============================================================
# 📌 LISTAGEM PRINCIPAL
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
# ➕ CADASTRAR CHIP (INSERT REAL)
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    data = request.form.to_dict()

    sql = f"""
        INSERT INTO `{PROJECT}.{DATASET}.dim_chip`
        (id_chip, numero, operadora, plano, status, observacao, created_at, updated_at, ativo)
        VALUES
        (@id_chip, @numero, @operadora, @plano, @status, @observacao,
         CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE)
    """

    params = [
        bigquery.ScalarQueryParameter("id_chip", "STRING", data.get("id_chip")),
        bigquery.ScalarQueryParameter("numero", "STRING", data.get("numero")),
        bigquery.ScalarQueryParameter("operadora", "STRING", data.get("operadora")),
        bigquery.ScalarQueryParameter("plano", "STRING", data.get("plano")),
        bigquery.ScalarQueryParameter("status", "STRING", data.get("status")),
        bigquery.ScalarQueryParameter("observacao", "STRING", data.get("observacao")),
    ]

    call_sp(sql, params)

    return """
        <script>
            alert('Chip cadastrado com sucesso!');
            window.location.href='/chips';
        </script>
    """


# ============================================================
# 🔍 BUSCAR CHIP (MODAL)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    df = bq.run_df(
        f"SELECT * FROM `{PROJECT}.{DATASET}.vw_chips_painel` WHERE sk_chip = @sk_chip",
        params=[bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip)]
    )

    if df.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    return jsonify(sanitize_df(df).iloc[0].to_dict())


# ============================================================
# 💾 SALVAR EDIÇÃO (COM AUDITORIA REAL)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    payload = request.json or {}
    sk_chip = payload.get("sk_chip")

    if not sk_chip:
        return jsonify({"error": "sk_chip ausente"}), 400

    df_atual = bq.run_df(
        f"SELECT * FROM `{PROJECT}.{DATASET}.dim_chip` WHERE sk_chip = @sk_chip",
        params=[bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip)]
    )

    if df_atual.empty:
        return jsonify({"error": "Chip não encontrado"}), 404

    atual = df_atual.iloc[0].to_dict()

    # ================================
    # 🔹 ATUALIZA DADOS BÁSICOS
    # ================================
    campos = ["numero", "operadora", "plano", "observacao"]
    houve_alteracao = any(payload.get(c) != atual.get(c) for c in campos)

    if houve_alteracao:
        sql = f"""
            CALL `{PROJECT}.{DATASET}.sp_upsert_chip`(
                @sk_chip,
                @numero,
                @operadora,
                @plano,
                @observacao
            )
        """

        params = [
            bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
            bigquery.ScalarQueryParameter("numero", "STRING", payload.get("numero")),
            bigquery.ScalarQueryParameter("operadora", "STRING", payload.get("operadora")),
            bigquery.ScalarQueryParameter("plano", "STRING", payload.get("plano")),
            bigquery.ScalarQueryParameter("observacao", "STRING", payload.get("observacao")),
        ]

        call_sp(sql, params)

        for campo in campos:
            if payload.get(campo) != atual.get(campo):
                call_sp(
                    f"""
                    CALL `{PROJECT}.{DATASET}.sp_registrar_evento_chip`(
                        @sk_chip,
                        'DADOS',
                        'ALTERACAO_CAMPO',
                        @campo,
                        @valor_antigo,
                        @valor_novo,
                        'Painel',
                        'Alteração via painel'
                    )
                    """,
                    [
                        bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                        bigquery.ScalarQueryParameter("campo", "STRING", campo),
                        bigquery.ScalarQueryParameter("valor_antigo", "STRING", str(atual.get(campo))),
                        bigquery.ScalarQueryParameter("valor_novo", "STRING", str(payload.get(campo))),
                    ]
                )

    # ================================
    # 🔹 STATUS
    # ================================
    if payload.get("status") and payload["status"] != atual.get("status"):
        call_sp(
            f"""
            CALL `{PROJECT}.{DATASET}.sp_alterar_status_chip`(
                @sk_chip,
                @status,
                'Painel',
                'Alteração via painel'
            )
            """,
            [
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("status", "STRING", payload.get("status")),
            ]
        )

    # ================================
    # 🔹 APARELHO
    # ================================
    if payload.get("sk_aparelho_atual") != atual.get("sk_aparelho_atual"):
        call_sp(
            f"""
            CALL `{PROJECT}.{DATASET}.sp_vincular_aparelho_chip`(
                @sk_chip,
                @sk_aparelho,
                'Painel',
                'Troca de aparelho'
            )
            """,
            [
                bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip),
                bigquery.ScalarQueryParameter("sk_aparelho", "INT64", payload.get("sk_aparelho_atual")),
            ]
        )

    return jsonify({"success": True})


# ============================================================
# 🧵 TIMELINE
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    df = bq.run_df(
        f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
        WHERE sk_chip = @sk_chip
        ORDER BY data_evento DESC
        """,
        params=[bigquery.ScalarQueryParameter("sk_chip", "INT64", sk_chip)]
    )

    return jsonify(sanitize_df(df).to_dict(orient="records"))
