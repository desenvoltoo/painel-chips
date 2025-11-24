# -*- coding: utf-8 -*-
"""
Painel Chips & Aparelhos — Flask + BigQuery + Cloud Run
"""

import os
from flask import Flask, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.chips import inserir_chip, listar_chips
from utils.aparelhos import inserir_aparelho, listar_aparelhos
from utils.relacionamentos import listar_eventos, registrar_evento

app = Flask(__name__)

bq = BigQueryClient()

# ==============================
# DASHBOARD
# ==============================
@app.route("/")
def dashboard():
    return render_template(
        "dashboard.html",
        tabela=bq.get_view(),
        kpis=bq.get_kpis()
    )

# ==============================
# CHIPS
# ==============================
@app.route("/chips")
def chips():
    dados = listar_chips()
    return render_template("chips.html", chips=dados)

@app.route("/chips/add", methods=["POST"])
def chips_add():
    inserir_chip(request.form)
    return jsonify({"status": "ok"})

# ==============================
# APARELHOS
# ==============================
@app.route("/aparelhos")
def aparelhos():
    return render_template("aparelhos.html", aparelhos=listar_aparelhos())

@app.route("/aparelhos/add", methods=["POST"])
def aparelhos_add():
    inserir_aparelho(request.form)
    return jsonify({"status": "ok"})

# ==============================
# EVENTOS / MOVIMENTAÇÃO
# ==============================
@app.route("/eventos")
def eventos():
    return render_template(
        "relacionamentos.html",
        rel=listar_eventos(),
        chips=listar_chips(),
        aparelhos=listar_aparelhos()
    )

@app.route("/eventos/add", methods=["POST"])
def eventos_add():
    registrar_evento(request.form)
    return jsonify({"status": "ok"})

# ==============================
# MAIN — CLOUD RUN
# ==============================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
