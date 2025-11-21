from flask import Flask, render_template
from utils.chips import listar_chips
from utils.aparelhos import listar_aparelhos

app = Flask(__name__)

@app.route("/")
def home():
    chips = listar_chips()
    aparelhos = listar_aparelhos()
    return render_template("chips.html", chips=chips, aparelhos=aparelhos)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
