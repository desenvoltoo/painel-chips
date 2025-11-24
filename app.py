from flask import Flask, render_template, request, redirect
from utils.bigquery_client import BigQueryClient

app = Flask(__name__)
bq = BigQueryClient()

# DASHBOARD
@app.route("/")
def home():
    dados = bq.get_view()
    return render_template("dashboard.html", tabela=dados.to_dict(orient="records"))

# APARELHOS LISTA + FORM
@app.route("/aparelhos")
def aparelhos():
    aparelhos = bq.get_aparelhos()
    return render_template(
        "aparelhos.html",
        aparelhos=aparelhos.to_dict(orient="records")
    )

# ADICIONAR APARELHO
@app.route("/aparelhos/add", methods=["POST"])
def add_aparelho():
    bq.insert_aparelho(request.form)
    return redirect("/aparelhos")

# CHIPS LISTA + FORM
@app.route("/chips")
def chips():
    chips_df = bq.get_chips()
    aparelhos_df = bq.get_aparelhos()

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )

# ADICIONAR CHIP
@app.route("/chips/add", methods=["POST"])
def add_chip():
    bq.insert_chip(request.form)
    return redirect("/chips")

# MOVIMENTAÇÃO
@app.route("/movimentacao")
def movimentacao():
    eventos = bq.get_eventos()
    return render_template(
        "movimentacao.html",
        eventos=eventos.to_dict(orient="records")
    )

# ADICIONAR EVENTO
@app.route("/movimentacao/add", methods=["POST"])
def add_evento():
    bq.insert_evento(request.form)
    return redirect("/movimentacao")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
