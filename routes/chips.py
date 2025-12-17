# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
import os

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()

PROJECT = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")


# ============================================================
# 🔐 REGISTRO DE HISTÓRICO / EVENTOS DO CHIP
# ============================================================
def registrar_evento_chip(
    sk_chip,
    tipo_evento,
    campo=None,
    valor_antigo=None,
    valor_novo=None,
    categoria="CHIP",
    origem="Painel",
    observacao=None
):
    sql = f"""
    INSERT INTO `{PROJECT}.{DATASET}.f_chip_evento`
    (
        sk_chip,
        categoria,
        tipo_evento,
        campo,
        valor_antigo,
        valor_novo,
        origem,
        observacao,
        data_evento,
        created_at
    )
    VALUES (
        {sk_chip},
        '{categoria}',
        '{tipo_evento}',
        {f"'{campo}'" if campo else "NULL"},
        {f"'{valor_antigo}'" if valor_antigo is not None else "NULL"},
        {f"'{valor_novo}'" if valor_novo is not None else "NULL"},
        '{origem}',
        {f"'{observacao}'" if observacao else "NULL"},
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    )
    """
    # EXECUÇÃO CORRETA PARA O SEU BigQueryClient
    bq.client.query(sql).result()


# ============================================================
# 📌 LISTAGEM PRINCIPAL
# ============================================================
@chips_bp.route("/chips")
def chips_list():
    try:
        chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
        aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

        return render_template(
            "chips.html",
            chips=chips_df.to_dict(orient="records"),
            aparelhos=aparelhos_df.to_dict(orient="records")
        )

    except Exception as e:
        print("🚨 Erro ao carregar chips:", e)
        return "Erro ao carregar chips", 500


# ============================================================
# ➕ CADASTRAR CHIP
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        data = request.form.to_dict()

        # normaliza vazios
        for k in list(data.keys()):
            if data[k] == "":
                data[k] = None

        # FRONT → MODELO FÍSICO
        if "data_inicio" in data:
            data["dt_inicio"] = data.pop("data_inicio")

        if "sk_aparelho_atual" in data and not data["sk_aparelho_atual"]:
            data["sk_aparelho_atual"] = None

        data.pop("sk_chip", None)

        # grava chip
        bq.upsert_chip(data)

        # recupera SK criado
        df = bq._run(f"""
            SELECT sk_chip
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE numero = '{data.get("numero")}'
            ORDER BY created_at DESC
            LIMIT 1
        """)

        if not df.empty:
            registrar_evento_chip(
                sk_chip=int(df.iloc[0]["sk_chip"]),
                tipo_evento="CRIACAO",
                observacao="Chip cadastrado via painel"
            )

        return """
            <script>
                alert('Chip cadastrado com sucesso!');
                window.location.href='/chips';
            </script>
        """

    except Exception as e:
        print("🚨 Erro ao cadastrar chip:", e)
        return "Erro ao cadastrar chip", 500


# ============================================================
# 🔍 BUSCAR CHIP (MODAL EDIÇÃO)
# ============================================================
@chips_bp.route("/chips/sk/<int:sk_chip>")
def chips_get_by_sk(sk_chip):
    try:
        df = bq._run(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chips_painel`
            WHERE sk_chip = {sk_chip}
            LIMIT 1
        """)

        if df.empty:
            return jsonify({"error": "Chip não encontrado"}), 404

        return jsonify(sanitize_df(df).iloc[0].to_dict())

    except Exception as e:
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"error": "Erro interno"}), 500


# ============================================================
# 💾 SALVAR EDIÇÃO (JSON)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        data = request.json or {}

        if not data.get("sk_chip"):
            return jsonify({"error": "sk_chip não informado"}), 400

        sk_chip = data["sk_chip"]

        # normaliza vazios
        for k in list(data.keys()):
            if data[k] == "":
                data[k] = None

        if "data_inicio" in data:
            data["dt_inicio"] = data.pop("data_inicio")

        # estado ANTES
        antes = bq._run(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip = {sk_chip}
        """).iloc[0].to_dict()

        # atualiza chip
        bq.upsert_chip(data)

        # estado DEPOIS
        depois = bq._run(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.dim_chip`
            WHERE sk_chip = {sk_chip}
        """).iloc[0].to_dict()

        # registra diferenças
        for campo, valor_novo in depois.items():
            valor_antigo = antes.get(campo)
            if str(valor_antigo) != str(valor_novo):
                registrar_evento_chip(
                    sk_chip=sk_chip,
                    tipo_evento="ALTERACAO",
                    campo=campo,
                    valor_antigo=str(valor_antigo),
                    valor_novo=str(valor_novo),
                    observacao="Atualização via painel"
                )

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro ao atualizar chip:", e)
        return jsonify({"error": "Erro ao salvar"}), 500


# ============================================================
# 🧵 TIMELINE / HISTÓRICO
# ============================================================
@chips_bp.route("/chips/timeline/<int:sk_chip>")
def chips_timeline(sk_chip):
    try:
        df = bq._run(f"""
            SELECT *
            FROM `{PROJECT}.{DATASET}.vw_chip_timeline`
            WHERE sk_chip = {sk_chip}
            ORDER BY data_evento DESC
        """)

        return jsonify(sanitize_df(df).to_dict(orient="records"))

    except Exception as e:
        print("🚨 Erro ao carregar timeline:", e)
        return jsonify([]), 500
