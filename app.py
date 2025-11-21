# app.py — Painel Chips (Flask + BigQuery)
from flask import Flask, render_template, request, jsonify
from utils.bigquery_client import (
    listar_chips,
    salvar_chip,
    listar_aparelhos,
    salvar_aparelho,
    listar_recargas,
    salvar_recarga,
    dashboard_resumo
)

app = Flask(__name__)

# ============================
# ROTAS DO PAINEL (HTML)
# ============================

@app.route("/")
def home():
    return render_template("dashboard.html")

@app.route("/chips")
def chips():
    return render_template("chips.html")

@app.route("/aparelhos")
def aparelhos():
    return render_template("aparelhos.html")

@app.route("/recargas")
def recargas():
    return render_template("recargas.html")


# ============================
# ROTAS DA API
# ============================

# ------ CHIPS ------
@app.route("/api/chips/listar")
def api_listar_chips():
    return jsonify(listar_chips())

@app.route("/api/chips/salvar", methods=["POST"])
def api_salvar_chip():
    data = request.get_json()
    salvar_chip(data)
    return jsonify({"status": "ok"})


# ------ APARELHOS ------
@app.route("/api/aparelhos/listar")
def api_listar_aparelhos():
    return jsonify(listar_aparelhos())

@app.route("/api/aparelhos/salvar", methods=["POST"])
def api_salvar_aparelho():
    data = request.get_json()
    salvar_aparelho(data)
    return jsonify({"status": "ok"})


# ------ RECARGAS ------
@app.route("/api/recargas/listar")
def api_listar_recargas():
    return jsonify(listar_recargas())

@app.route("/api/recargas/salvar", methods=["POST"])
def api_salvar_recarga():
    data = request.get_json()
    salvar_recarga(data)
    return jsonify({"status": "ok"})


# ------ DASHBOARD ------
@app.route("/api/dashboard/resumo")
def api_dashboard_resumo():
    return jsonify(dashboard_resumo())


# ============================
# MAIN
# ============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
