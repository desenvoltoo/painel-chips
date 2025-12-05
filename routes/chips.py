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

    if "T" in value:
        return value.split("T")[0]

    if "/" in value:
        try:
            d, m, y = value.split("/")
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except:
            pass

    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]

    try:
        return datetime.fromisoformat(value.replace("Z", "")).strftime("%Y-%m-%d")
    except:
        return None



# ======================================================================
# LISTAR PÁGINA PRINCIPAL
# ======================================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = sanitize_df(bq.get_view("vw_chips_painel"))
    aparelhos_df = sanitize_df(bq.get_view("vw_aparelhos"))

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )



# ======================================================================
# ADICIONAR CHIP
# ======================================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    dados = request.form.to_dict()
    bq.upsert_chip(dados)
    return redirect("/chips")



# ======================================================================
# BUSCAR CHIP PARA EDIÇÃO — SOMENTE POR sk_chip (AGORA CORRIGIDO)
# ======================================================================
@chips_bp.route("/chips/sk/<sk_chip>")
def get_chip(sk_chip):
    df = sanitize_df(bq.get_view("vw_chips_painel"))
    resultado = df[df["sk_chip"].astype(str) == str(sk_chip)]

    if resultado.empty:
        return jsonify({"erro": "Chip não encontrado"}), 404

    chip = resultado.to_dict(orient="records")[0]

    chip["dt_inicio"] = format_date(chip.get("dt_inicio"))
    chip["ultima_recarga_data"] = format_date(chip.get("ultima_recarga_data"))

    return jsonify(chip)



# ======================================================================
# UPDATE VIA MODAL — EVENTOS + MOVIMENTAÇÃO
# ======================================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    dados = request.json

    if not dados or "sk_chip" not in dados:
        return jsonify({"success": False, "erro": "sk_chip ausente"}), 400

    df = sanitize_df(bq.get_view("vw_chips_painel"))
    atual = df[df["sk_chip"].astype(str) == str(dados["sk_chip"])]

    if atual.empty:
        return jsonify({"success": False, "erro": "Chip não encontrado"}), 404

    atual = atual.iloc[0]

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

    for campo, label in campos_evento:
        old = str(atual.get(campo) or "")
        new = str(dados.get(campo) or "")
        if old != new:
            bq.registrar_evento_chip(
                sk_chip=int(atual["sk_chip"]),
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
                sk_chip=int(atual["sk_chip"]),
                sk_aparelho=int(dados["sk_aparelho_atual"]),
                tipo="TROCA_APARELHO",
                origem="Painel",
                observacao="Alteração via editor"
            )

    # Atualização final via upsert
    bq.upsert_chip(dados)

    return jsonify({"success": True})



# ======================================================================
# REGISTRO MANUAL DE MOVIMENTAÇÃO
# ======================================================================
@chips_bp.route("/chips/movimento", methods=["POST"])
def chips_movimento():
    dados = request.json

    ok = bq.registrar_movimento_chip(
        sk_chip=dados.get("sk_chip"),
        sk_aparelho=dados.get("sk_aparelho"),
        tipo=dados.get("tipo"),
        origem=dados.get("origem", "Painel"),
        observacao=dados.get("observacao", "")
    )

    return jsonify({"success": ok})
