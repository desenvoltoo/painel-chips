# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# =============================================================================
# 📌 LISTAR CHIPS – PÁGINA PRINCIPAL
# =============================================================================
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
        print("🚨 Erro ao carregar /chips:", e)
        return "Erro ao carregar chips", 500


# =============================================================================
# ➕ ADICIONAR NOVO CHIP
# =============================================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        dados = request.form.to_dict()
        bq.upsert_chip(dados)
        return redirect("/chips")

    except Exception as e:
        print("🚨 Erro ao adicionar chip:", e)
        return "Erro ao adicionar chip", 500


# =============================================================================
# 🔍 API — OBTER CHIP ESPECÍFICO (JSON)
# =============================================================================
@chips_bp.route("/chips/<id_chip>")
def get_chip(id_chip):
    try:
        df = sanitize_df(bq.get_view("vw_chips_painel"))
        df = df[df["id_chip"].astype(str) == str(id_chip)]

        if df.empty:
            return jsonify({"erro": "Chip não encontrado"}), 404

        return jsonify(df.to_dict(orient="records")[0])

    except Exception as e:
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"erro": "Erro interno"}), 500


# =============================================================================
# 🔥 UPDATE VIA MODAL (AJAX / JSON) — COM REGISTRO DE EVENTOS
# =============================================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        dados = request.json

        if not dados or "id_chip" not in dados:
            return jsonify({"success": False, "erro": "Dados inválidos"}), 400

        # 1️⃣ Buscar estado anterior do chip
        df = sanitize_df(bq.get_view("vw_chips_painel"))
        atual = df[df["id_chip"].astype(str) == str(dados["id_chip"])]

        if atual.empty:
            return jsonify({"success": False, "erro": "Chip não encontrado"}), 404

        atual = atual.iloc[0]

        # 2️⃣ Campos monitorados para histórico (EVENTOS)
        campos_evento = [
            ("numero", "NÚMERO"),
            ("operadora", "OPERADORA"),
            ("operador", "OPERADOR"),
            ("plano", "PLANO"),
            ("status", "STATUS"),
            ("observacao", "OBSERVAÇÃO"),
            ("dt_inicio", "DATA_INICIO"),
            ("ultima_recarga_valor", "VALOR_RECARGA"),
            ("ultima_recarga_data", "DATA_RECARGA"),
            ("total_gasto", "TOTAL_GASTO"),
        ]

        # 3️⃣ Registrar eventos quando houver alteração
        for campo, label in campos_evento:
            antigo = str(atual.get(campo) or "")
            novo = str(dados.get(campo) or "")

            if antigo != novo:
                bq.registrar_evento_chip(
                    sk_chip=int(atual["sk_chip"]),
                    tipo_evento=label,
                    valor_antigo=antigo,
                    valor_novo=novo,
                    origem="Painel",
                    obs="Alteração via editor"
                )

        # 4️⃣ Se o aparelho mudou → registra movimento
        antigo_ap = atual.get("sk_aparelho_atual")
        novo_ap = dados.get("sk_aparelho_atual")

        if str(antigo_ap) != str(novo_ap):
            if novo_ap not in (None, "", "None"):
                bq.registrar_movimento_chip(
                    sk_chip=int(atual["sk_chip"]),
                    sk_aparelho=int(novo_ap),
                    tipo="TROCA_APARELHO",
                    origem="Painel",
                    observacao="Alteração via editor"
                )

        # 5️⃣ Agora faz o UPDATE real
        bq.upsert_chip(dados)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro update-json:", e)
        return jsonify({"success": False, "erro": str(e)}), 500


# =============================================================================
# 🔄 REGISTRAR MOVIMENTO MANUAL (CASO NECESSÁRIO NO FUTURO)
# =============================================================================
@chips_bp.route("/chips/movimento", methods=["POST"])
def chips_movimento():
    try:
        dados = request.json

        ok = bq.registrar_movimento_chip(
            sk_chip=dados.get("sk_chip"),
            sk_aparelho=dados.get("sk_aparelho"),
            tipo=dados.get("tipo"),
            origem=dados.get("origem", "Painel"),
            observacao=dados.get("observacao", "")
        )

        return jsonify({"success": ok})

    except Exception as e:
        print("🚨 Erro movimento chip:", e)
        return jsonify({"success": False, "erro": str(e)}), 500
