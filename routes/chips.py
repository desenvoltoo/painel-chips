# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df
from datetime import datetime

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# ======================================================================
# NORMALIZADOR DE DATAS
# ======================================================================
def format_date(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    value = str(value)

    # ISO com T
    if "T" in value:
        return value.split("T")[0]

    # DD/MM/YYYY
    if "/" in value:
        try:
            d, m, y = value.split("/")
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except Exception:
            pass

    # YYYY-MM-DD
    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]

    # Fallback
    try:
        return datetime.fromisoformat(value.replace("Z", "")).strftime("%Y-%m-%d")
    except Exception:
        return None


# ======================================================================
# LISTAR PÁGINA PRINCIPAL
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
# ADICIONAR CHIP
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
# BUSCAR CHIP PARA EDIÇÃO — POR id_chip (ALINHADO À VIEW)
# ======================================================================
@chips_bp.route("/chips/<id_chip>")
def get_chip(id_chip):
    try:
        df = sanitize_df(bq.get_view("vw_chips_painel"))
        resultado = df[df["id_chip"].astype(str) == str(id_chip)]

        if resultado.empty:
            return jsonify({"erro": "Chip não encontrado"}), 404

        chip = resultado.to_dict(orient="records")[0]

        # Normalizar datas antes de mandar pro front
        chip["dt_inicio"] = format_date(chip.get("dt_inicio"))
        chip["ultima_recarga_data"] = format_date(chip.get("ultima_recarga_data"))

        return jsonify(chip)

    except Exception as e:
        print("🚨 Erro ao buscar chip:", e)
        return jsonify({"erro": "Erro interno"}), 500


# ======================================================================
# UPDATE VIA CARD DE EDIÇÃO — EVENTOS + MOVIMENTAÇÃO
# ======================================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        dados = request.json or {}

        # Agora validamos por id_chip (que vem do form)
        if "id_chip" not in dados or not dados["id_chip"]:
            return jsonify({"success": False, "erro": "id_chip ausente"}), 400

        df = sanitize_df(bq.get_view("vw_chips_painel"))
        atual = df[df["id_chip"].astype(str) == str(dados["id_chip"])]

        if atual.empty:
            return jsonify({"success": False, "erro": "Chip não encontrado"}), 404

        atual = atual.iloc[0]

        # Tenta pegar sk_chip da view (se existir)
        sk_chip_view = atual.get("sk_chip", None)

        # Campos monitorados para eventos
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

        # Se tiver sk_chip disponível, registra eventos
        if sk_chip_view is not None:
            for campo, label in campos_evento:
                old = str(atual.get(campo) or "")
                new = str(dados.get(campo) or "")
                if old != new:
                    bq.registrar_evento_chip(
                        sk_chip=int(sk_chip_view),
                        tipo_evento=label,
                        valor_antigo=old,
                        valor_novo=new,
                        origem="Painel",
                        obs="Alteração via editor"
                    )

            # Troca de aparelho
            if str(atual.get("sk_aparelho_atual")) != str(dados.get("sk_aparelho_atual")):
                if dados.get("sk_aparelho_atual"):
                    bq.registrar_movimento_chip(
                        sk_chip=int(sk_chip_view),
                        sk_aparelho=int(dados["sk_aparelho_atual"]),
                        tipo="TROCA_APARELHO",
                        origem="Painel",
                        observacao="Alteração via editor"
                    )

        # Atualização final via upsert (usa id_chip e demais campos enviados)
        bq.upsert_chip(dados)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro update-json:", e)
        return jsonify({"success": False, "erro": str(e)}), 500


# ======================================================================
# REGISTRO MANUAL DE MOVIMENTAÇÃO
# ======================================================================
@chips_bp.route("/chips/movimento", methods=["POST"])
def chips_movimento():
    try:
        dados = request.json or {}

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
