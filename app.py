from flask import Flask, render_template, request, redirect
from utils.bigquery_client import BigQueryClient

app = Flask(__name__)
bq = BigQueryClient()

# =======================
# ROTA PRINCIPAL (Dashboard)
# =======================
@app.route("/")
def home():
    kpis = bq.get_kpis()
    recargas = bq.get_ultimas_recargas()
    tabela = bq.get_view()

    return render_template(
        "dashboard.html",
        kpis=kpis,
        recargas=recargas.to_dict(orient="records"),
        tabela=tabela.to_dict(orient="records")
    )

# =======================
# LISTA DE APARELHOS
# =======================
@app.route("/aparelhos")
def aparelhos():
    dados = bq.get_aparelhos()
    return render_template(
        "aparelhos.html",
        aparelhos=dados.to_dict(orient="records")
    )

# =======================
# ADICIONAR APARELHO (POST)
# =======================
@app.route("/aparelhos/add", methods=["POST"])
def add_aparelho():
    data = request.form.to_dict()

    # Mapeamento direto para BigQuery
    row = {
        "nome": data.get("nome"),
        "marca": data.get("marca"),
        "modelo": data.get("modelo"),
        "imei": data.get("imei"),
        "status": data.get("status"),
    }

    bq.insert_aparelho(row)
    return redirect("/aparelhos")

# =======================
# LISTA DE CHIPS
# =======================
@app.route("/chips")
def chips():
    chips_df = bq.get_chips()
    aparelhos_df = bq.get_aparelhos()

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )

# =======================
# ADICIONAR CHIP (POST)
# =======================
@app.route("/chips/add", methods=["POST"])
def add_chip():
    data = request.form.to_dict()

    row = {
        "id_chip": data.get("id_chip"),
        "numero": data.get("numero"),
        "operadora": data.get("operadora"),
        "plano": data.get("plano"),
        "status": data.get("status"),
        "dt_inicio": data.get("dt_inicio"),
        "ultima_recarga_valor": float(data.get("ultima_recarga_valor") or 0),
        "ultima_recarga_data": data.get("ultima_recarga_data"),
        "total_gasto": float(data.get("total_gasto") or 0),
        "sk_aparelho_atual": int(data.get("sk_aparelho_atual")) if data.get("sk_aparelho_atual") else None
    }

    bq.insert_chip(row)
    return redirect("/chips")

# =======================
# MOVIMENTAÇÕES
# =======================
@app.route("/movimentacao")
def movimentacao():
    dados = bq.get_view()

    return render_template(
        "movimentacao.html",
        dados=dados.to_dict(orient="records")
    )

# =======================
# START FLASK
# =======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
