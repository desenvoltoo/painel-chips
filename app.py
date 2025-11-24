from flask import Flask, render_template
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
    return render_template("aparelhos.html", aparelhos=dados.to_dict(orient="records"))

# =======================
# LISTA DE CHIPS
# =======================
@app.route("/chips")
def chips():
    dados = bq.get_chips()
    return render_template("chips.html", chips=dados.to_dict(orient="records"))

# =======================
# START FLASK
# =======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
