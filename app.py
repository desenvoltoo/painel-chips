# -*- coding: utf-8 -*-
"""
Painel Chips & Aparelhos — Flask + BigQuery + Cloud Run
"""

import os
from flask import Flask, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient
from utils.chips import listar_chips, inserir_chip
from utils.aparelhos import listar_aparelhos, inserir_aparelho
from utils.relacionamentos import listar_relacionamentos, vincular_chip, desvincular_chip

# ==============================
# CONFIGURAÇÃO DO FLASK
# ==============================
app = Flask(__name__)

# Instância única do BigQuery
bq = BigQueryClient()

# ==============================
# ROTAS DO SISTEMA
# ==============================

@app.route("/")
def home():
    return render_template("dashboard.html",
        kpis=bq.kpis_dashboard(),
        ultimas_recargas=bq.get_ultimas_recargas(),
        tabela=bq.get_view_painel()
    )

# -------------------------------
# CHIPS
# -------------------------------
@app.route("/chips")
def chips():
    chips = listar_chips()
    return render_template("chips.html", chips=chips)

@app.route("/chips/add", methods=["POST"])
def chips_add():
    data = request.form.to_dict()
    inserir_chip(data)
    return jsonify({"status": "ok"})

# -------------------------------
# APARELHOS
# -------------------------------
@app.route("/aparelhos")
def aparelhos():
    aparelhos = listar_aparelhos()
    return render_template("aparelhos.html", aparelhos=aparelhos)

@app.route("/aparelhos/add", methods=["POST"])
def aparelhos_add():
    data = request.form.to_dict()
    inserir_aparelho(data)
    return jsonify({"status": "ok"})

# -------------------------------
# VÍNCULOS (Chip ↔ Aparelho)
# -------------------------------
@app.route("/relacionamentos")
def rel():
    relacionamentos = listar_relacionamentos()
    chips = listar_chips()
    aparelhos = listar_aparelhos()
    return render_template("relacionamentos.html",
                           rel=relacionamentos,
                           chips=chips,
                           aparelhos=aparelhos)

@app.route("/vincular", methods=["POST"])
def vincular():
    vincular_chip(request.form["sk_chip"], request.form["sk_aparelho"])
    return jsonify({"status": "ok"})

@app.route("/desvincular", methods=["POST"])
def desvincular():
    desvincular_chip(request.form["id"])
    return jsonify({"status": "ok"})

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
