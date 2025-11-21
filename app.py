# -*- coding: utf-8 -*-
"""
app.py — Painel Chips Integrado ao BigQuery (Modelo Estrela Moderno)

Rotas:
- /                         → home (lista chips + aparelhos)
- /chip/<sk_chip>           → histórico de eventos do chip
- /recarga (POST)           → registrar recarga
- /vincular (POST)          → vincular chip a aparelho
- /desvincular (POST)       → desvincular chip de aparelho
"""

import os
from flask import (
    Flask, render_template, request, jsonify
)
from utils.chips import get_chips, get_chip_eventos
from utils.aparelhos import listar_aparelhos
from utils.recargas import adicionar_recarga
from utils.relacionamentos import vincular, desvincular

# ---------------------------------------------
# Configuração do Flask
# ---------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "supersecret")


# ---------------------------------------------
# ROTA PRINCIPAL — LISTA CHIP + APARELHOS
# ---------------------------------------------
@app.route("/")
def index():
    chips = get_chips()
    aparelhos = listar_aparelhos()

    return render_template(
        "chips.html",
        chips=chips,
        aparelhos=aparelhos
    )


# ---------------------------------------------
# HISTÓRICO DE EVENTOS DO CHIP
# ---------------------------------------------
@app.route("/chip/<int:sk_chip>")
def historico_chip(sk_chip):
    eventos = get_chip_eventos(sk_chip)
    return jsonify(eventos)


# ---------------------------------------------
# REGISTRAR RECARGA
# ---------------------------------------------
@app.route("/recarga", methods=["POST"])
def route_recarga():
    form = request.json

    adicionar_recarga(
        form["sk_chip"],
        form["valor"],
        form["data"],
        form.get("origem", "painel"),
        form.get("observacao", "")
    )

    return jsonify({"ok": True, "msg": "Recarga registrada com sucesso."})


# ---------------------------------------------
# VINCULAR CHIP A APARELHO
# ---------------------------------------------
@app.route("/vincular", methods=["POST"])
def route_vincular():
    form = request.json

    vincular(
        form["sk_chip"],
        form["sk_aparelho"],
        form["data"],
        form.get("origem", "painel"),
        form.get("obs", "")
    )

    return jsonify({"ok": True, "msg": "Chip vinculado ao aparelho."})


# ---------------------------------------------
# DESVINCULAR CHIP
# ---------------------------------------------
@app.route("/desvincular", methods=["POST"])
def route_desvincular():
    form = request.json

    desvincular(
        form["sk_chip"],
        form["data"],
        form.get("origem", "painel"),
        form.get("obs", "")
    )

    return jsonify({"ok": True, "msg": "Chip desvinculado."})


# ---------------------------------------------
# SAÚDE DO SISTEMA (Cloud Run)
# ---------------------------------------------
@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------
# INICIALIZAÇÃO
# ---------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
