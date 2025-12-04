# routes/chips.py

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
# 🔍 API — OBTER CHIP ESPECÍFICO (JSON) — para MODAL
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
# 🔄 UPDATE COMPLETO VIA MODAL (JSON)
#     → Atualiza chip
#     → Identifica campos alterados
#     → Grava evento automático na f_chip_evento
# =============================================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        dados = request.json

        if not dados or "id_chip" not in dados:
            return jsonify({"success": False, "erro": "Dados inválidos"}), 400

        # 1) Recupera estado atual
        atual_df = sanitize_df(bq.get_view("vw_chips_painel"))
        atual = atual_df[atual_df["id_chip"].astype(str) == str(dados["id_chip"])]

        if atual.empty:
            return jsonify({"success": False, "erro": "Chip não encontrado"}), 404

        atual = atual.to_dict(orient="records")[0]

        # 2) Aplica update normal
        bq.upsert_chip(dados)

        # 3) Detecta mudanças campo a campo
        campos_monitorados = {
            "status": "STATUS",
            "operadora": "OPERADORA",
            "plano": "PLANO",
            "sk_aparelho_atual": "APARELHO"
        }

        eventos = []

        for campo, nome_evento in campos_monitorados.items():
            antigo = str(atual.get(campo) or "")
            novo = str(dados.get(campo) or "")

            if antigo != novo:
                eventos.append((nome_evento, antigo, novo))

        # Nenhuma mudança → ok
        if not eventos:
            return jsonify({"success": True, "msg": "Sem alterações"})

        # 4) Para cada mudança → grava evento automático no BigQuery
        for ev in eventos:
            tipo_evento, valor_old, valor_new = ev

            sql = f"""
                CALL `{bq.project}.{bq.dataset}.sp_registrar_evento_chip`(
                    {atual['sk_chip']},
                    '{tipo_evento}',
                    '{valor_old}',
                    '{valor_new}',
                    'Painel',
                    'Alteração automática detectada pelo painel'
                );
            """
            bq._run(sql)

        return jsonify({"success": True})

    except Exception as e:
        print("🚨 Erro update-json:", e)
        return jsonify({"success": False, "erro": str(e)}), 500


# =============================================================================
# 🔄 REGISTRAR MOVIMENTO DE CHIP (TROCA DE APARELHO)
#     → USADO pelo botão de transferência
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
