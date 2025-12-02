# routes/aparelhos.py

from flask import Blueprint, render_template, request, redirect, jsonify, session
from utils.db import db_query, db_execute
from utils.auth_required import login_required

aparelhos_bp = Blueprint("aparelhos", __name__)


# =============================================================================
# 📌 LISTAR APARELHOS (multi-empresa)
# =============================================================================
@aparelhos_bp.route("/aparelhos")
@login_required
def aparelhos_list():
    id_empresa = session["id_empresa"]

    aparelhos = db_query("""
        SELECT *
        FROM aparelhos
        WHERE id_empresa = %s
        ORDER BY modelo ASC;
    """, (id_empresa,))

    return render_template(
        "aparelhos.html",
        aparelhos=aparelhos
    )


# =============================================================================
# ➕ ADICIONAR APARELHO
# =============================================================================
@aparelhos_bp.route("/aparelhos/add", methods=["POST"])
@login_required
def aparelhos_add():
    id_empresa = session["id_empresa"]

    id_aparelho = request.form.get("id_aparelho")
    modelo = request.form.get("modelo")
    marca = request.form.get("marca")
    imei = request.form.get("imei")
    status = request.form.get("status", "ATIVO")

    db_execute("""
        INSERT INTO aparelhos (
            id_empresa, id_aparelho, modelo, marca, imei, status, ativo
        )
        VALUES (%s, %s, %s, %s, %s, %s, TRUE);
    """, (
        id_empresa, id_aparelho, modelo, marca, imei, status
    ))

    return redirect("/aparelhos")


# =============================================================================
# 🔍 OBTER APARELHO (JSON) – Para modal
# =============================================================================
@aparelhos_bp.route("/aparelhos/<int:sk_aparelho>")
@login_required
def aparelhos_get(sk_aparelho):
    id_empresa = session["id_empresa"]

    aparelho = db_query("""
        SELECT *
        FROM aparelhos
        WHERE sk_aparelho = %s AND id_empresa = %s
        LIMIT 1;
    """, (sk_aparelho, id_empresa))

    if not aparelho:
        return jsonify({"erro": "Aparelho não encontrado"}), 404

    return jsonify(aparelho[0])


# =============================================================================
# ✏️ ATUALIZAR APARELHO (JSON via modal)
# =============================================================================
@aparelhos_bp.route("/aparelhos/update-json", methods=["POST"])
@login_required
def aparelhos_update():
    dados = request.json
    id_empresa = session["id_empresa"]

    sk_aparelho = dados.get("sk_aparelho")
    if not sk_aparelho:
        return jsonify({"success": False, "erro": "ID inválido"}), 400

    db_execute("""
        UPDATE aparelhos
        SET
            id_aparelho = %s,
            modelo = %s,
            marca = %s,
            imei = %s,
            status = %s,
            ativo = %s,
            updated_at = NOW()
        WHERE sk_aparelho = %s AND id_empresa = %s;
    """, (
        dados.get("id_aparelho"),
        dados.get("modelo"),
        dados.get("marca"),
        dados.get("imei"),
        dados.get("status"),
        dados.get("ativo", True),
        sk_aparelho,
        id_empresa
    ))

    return jsonify({"success": True})
