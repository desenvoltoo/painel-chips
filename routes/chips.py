# routes/chips.py
# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

chips_bp = Blueprint("chips", __name__)
bq = BigQueryClient()


# ============================================================
# 📌 LISTAR CHIPS (Página principal)
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
        print("❌ Erro ao carregar /chips:", e)
        return "Erro ao carregar chips", 500



# ============================================================
# ➕ CADASTRAR NOVO CHIP
# ============================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    try:
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
                '{dados.get("dt_inicio") or None}',
                {dados.get("ultima_recarga_valor") or "NULL"},
                '{dados.get("ultima_recarga_data") or None}',
                {dados.get("total_gasto") or "NULL"},
                {dados.get("sk_aparelho_atual") or "NULL"},
                '{dados.get("observacao")}'
            )
        """

        bq.execute_query(query)

        return "<script>alert('Chip cadastrado com sucesso!'); window.location.href='/chips';</script>"

    except Exception as e:
        print("❌ Erro ao cadastrar chip:", e)
        return "Erro ao inserir chip", 500



# ============================================================
# 🔍 BUSCAR CHIP PELO SK (Usado pelo modal)
# ============================================================
@chips_bp.route("/chips/sk/<sk_chip>")
def chips_get_by_sk(sk_chip):
    try:
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

    except Exception as e:
        print("❌ Erro ao buscar chip por SK:", e)
        return jsonify({"error": "Erro interno"}), 500



# ============================================================
# ✏️ ATUALIZAR CHIP (modal salva via JSON)
# ============================================================
@chips_bp.route("/chips/update-json", methods=["POST"])
def chips_update_json():
    try:
        data = request.json

        query = f"""
            UPDATE `painel-universidade.marts.dim_chip`
            SET
                numero = '{data.get("numero")}',
                operadora = '{data.get("operadora")}',
                operador = '{data.get("operador")}',
                status = '{data.get("status")}',
                plano = '{data.get("plano")}',
                dt_inicio = '{data.get("dt_inicio")}',
                ultima_recarga_data = '{data.get("ultima_recarga_data")}',
                ultima_recarga_valor = {data.get("ultima_recarga_valor") or "NULL"},
                total_gasto = {data.get("total_gasto") or "NULL"},
                sk_aparelho_atual = {data.get("sk_aparelho_atual") or "NULL"},
                observacao = '{data.get("observacao")}'
            WHERE sk_chip = {data.get("sk_chip")}
        """

        bq.execute_query(query)

        return jsonify({"success": True})

    except Exception as e:
        print("❌ Erro ao atualizar chip:", e)
        return jsonify({"success": False})
