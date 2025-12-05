# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from datetime import datetime

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# ======================================================================
# 🔧 NORMALIZADOR DE DATAS
# ======================================================================
def format_date(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    value = str(value)

    # YYYY-MM-DD
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]

    # ISO 2025-01-01T00:00:00
    if "T" in value:
        return value.split("T")[0]

    # DD/MM/YYYY
    if "/" in value:
        try:
            d, m, y = value.split("/")
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except:
            pass

    # Fallback
    try:
        dt = datetime.fromisoformat(value.replace("Z", ""))
        return dt.strftime("%Y-%m-%d")
    except:
        return None



# ======================================================================
# 📌 LISTAR PÁGINA PRINCIPAL DE CHIPS
# ======================================================================
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



# ======================================================================
# ➕ ADICIONAR CHIP NOVO
# ======================================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
        dados = request.form.to_dict()
        bq.upsert_chip(dados)
        return redirect("/chips")

    except Exception as e:
        print("🚨 Erro ao adicionar chip:", e)
        return "Erro ao adicionar chip", 500



# ======================================================================
# 🔍 API — OBTER CHIP PARA EDIÇÃO (BUSCA POR sk_chip PRIMEIRO)
# ======================================================================
@chips_bp.route("/chips/<value>")
def get_chip(value):
    try:
        df = sanitize_df(bq.get_view("vw_chips_painel"))

        chip = None

        # 1️⃣ Busca por sk_chip
        if "sk_chip" in df.columns:
            resultado = df[df["sk_chip"].astype(str) == str(value)]
            if not resultado.empty:
                chip = resultado.to_dict(orient="records")[0]

        # 2️⃣ Se não achar, busca por id_chip
        if chip is None:
            resultado = df[df["id_chip"].astype(str) == str(value)]
            if resultado.empty:
                return jsonify({"erro": "Chip não encontrado"}), 404
            chip = resultado.to_dict(orient="records")[0]

        # 3️⃣ Normaliza datas antes de enviar ao modal
        chip["dt_inicio"] = format_date(chip.get("dt_inicio"))
        chip["ultima_recarga_data"] = format_date(chip.get("ultima_recarga_data"))

        return jsonify(chip)

    except Exception as e:
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"erro": "Erro interno"}), 500



# ======================================================================
# 🔥 ATUALIZAR CHIP VIA MODAL (COM EVENTOS)
# ======================================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        dados = request.json

        if not dados or "id_chip" not in dados:
            return jsonify({"success": False, "erro": "Dados inválidos"}), 400

        # Buscar estado atual
        df = sanitize_df(bq.get_view("vw_chips_painel"))
        atual = df[df["id_chip"].astype(str) == str(dados["id_chip"])]

        if atual.empty:
            return jsonify({"success": False, "erro": "Chip não encontrado"}), 404

        atual = atual.iloc[0]

        # Campos monitorados
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

        # Registrar eventos
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

        # Troca de aparelho
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

        # Atualizar chip
        bq.upsert_chip(dados)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro update-json:", e)
        return jsonify({"success": False, "erro": str(e)}), 500



# ======================================================================
# 🔄 REGISTRAR MOVIMENTAÇÃO MANUAL
# ======================================================================
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
