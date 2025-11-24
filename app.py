from flask import Flask, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient

app = Flask(__name__)
bq = BigQueryClient()

# =======================
# PÁGINAS
# =======================

@app.route("/")
def dashboard():
    resumo = bq.dashboard_data()
    return render_template("dashboard.html", resumo=resumo)

@app.route("/chips")
def chips_page():
    chips = bq.get_chips()
    return render_template("chips.html", chips=chips)

@app.route("/aparelhos")
def aparelhos_page():
    aparelhos = bq.get_aparelhos()
    return render_template("aparelhos.html", aparelhos=aparelhos)

@app.route("/recargas")
def recargas_page():
    chips = bq.get_chips()
    return render_template("recargas.html", chips=chips)

# ============================
# API – CHIPS
# ============================

@app.route("/api/chip/add", methods=["POST"])
def api_add_chip():
    bq.insert_chip(request.json)
    return jsonify({"status": "ok"})

@app.route("/api/chip/update", methods=["POST"])
def api_update_chip():
    bq.update_chip(request.json)
    return jsonify({"status": "ok"})

# ============================
# API – RECARGA (última recarga)
# ============================

@app.route("/api/chip/recarga", methods=["POST"])
def api_update_recarga():
    bq.update_recarga(request.json)
    return jsonify({"status": "ok"})

# ============================
# API – APARELHOS
# ============================

@app.route("/api/aparelho/add", methods=["POST"])
def api_add_aparelho():
    bq.insert_aparelho(request.json)
    return jsonify({"status": "ok"})

@app.route("/api/aparelho/update", methods=["POST"])
def api_update_aparelho():
    bq.update_aparelho(request.json)
    return jsonify({"status": "ok"})

# ============================
# API – VÍNCULO CHIP/APARELHO
# ============================

@app.route("/api/vincular", methods=["POST"])
def api_vincular():
    bq.vincular_chip_aparelho(request.json)
    return jsonify({"status": "ok"})

@app.route("/api/desvincular", methods=["POST"])
def api_desvincular():
    bq.desvincular_chip_aparelho(request.json)
    return jsonify({"status": "ok"})

# ============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
