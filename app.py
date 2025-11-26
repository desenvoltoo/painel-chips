# -*- coding: utf-8 -*-
import os
from flask import Flask, render_template, request, jsonify
from utils.bigquery_client import BigQueryClient

app = Flask(__name__)

# ===========================
# CONFIGURAÇÃO
# ===========================
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
DATASET = os.getenv("BQ_DATASET", "marts")
PORT = int(os.getenv("PORT", 8080))

bq = BigQueryClient(PROJECT_ID, DATASET)


# ===========================
# ROTAS PRINCIPAIS
# ===========================

@app.route("/")
def home():
    return render_template("dashboard.html")


@app.route("/dashboard")
def dashboard():
    df = bq.get_view("vw_chips_painel")

    # Converte DataFrame → lista de dict
    tabela = df.to_dict(orient="records")

    # ==================== KPIs ====================
    total_chips = len(tabela)
    chips_ativos = sum(1 for x in tabela if (x["status"] or "").upper() == "ATIVO")
    disparando = sum(1 for x in tabela if (x["status"] or "").upper() == "DISPARANDO")
    banidos = sum(1 for x in tabela if (x["status"] or "").upper() == "BANIDO")

    # ==================== LISTAS ====================
    lista_status = sorted(list({(x["status"] or "").upper() for x in tabela if x["status"]}))
    lista_operadora = sorted(list({x["operadora"] for x in tabela if x["operadora"]}))

    # ==================== ALERTA RECARGA ====================
    alerta_query = """
        SELECT numero, status, operadora,
               ultima_recarga_data,
               DATE_DIFF(CURRENT_DATE(), DATE(ultima_recarga_data), DAY) AS dias_sem_recarga
        FROM `painel-universidade.marts.vw_chips_painel`
        WHERE ultima_recarga_data IS NOT NULL
          AND DATE_DIFF(CURRENT_DATE(), DATE(ultima_recarga_data), DAY) > 80
        ORDER BY dias_sem_recarga DESC
    """

    alerta = bq.query(alerta_query).to_dict(orient="records")
    qtd_alerta = len(alerta)

    return render_template(
        "dashboard.html",
        tabela=tabela,
        total_chips=total_chips,
        chips_ativos=chips_ativos,
        disparando=disparando,
        banidos=banidos,
        lista_status=lista_status,
        lista_operadora=lista_operadora,
        alerta_recarga=alerta,
        qtd_alerta=qtd_alerta
    )


# ===========================
# LISTAGEM DE CHIPS
# ===========================
@app.route("/chips")
def chips():
    df = bq.get_view("vw_chips_painel")
    tabela = df.to_dict(orient="records")
    return render_template("chips.html", tabela=tabela)


# ===========================
# LISTAGEM DE APARELHOS
# ===========================
@app.route("/aparelhos")
def aparelhos():
    df = bq.get_view("vw_aparelhos")
    tabela = df.to_dict(orient="records")
    return render_template("aparelhos.html", tabela=tabela)


# ===========================
# REGISTRO DE MOVIMENTAÇÃO
# ===========================
@app.route("/movimentacao", methods=["GET", "POST"])
def movimentacao():
    if request.method == "GET":
        chips = bq.get_view("vw_chips_painel").to_dict(orient="records")
        aparelhos = bq.get_view("vw_aparelhos").to_dict(orient="records")
        return render_template("movimentacao.html", chips=chips, aparelhos=aparelhos)

    # POST → registrar movimento
    data = request.form.to_dict()

    sql = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.f_chip_aparelho`
        (sk_chip, sk_aparelho, tipo_movimento, origem, observacao, data_uso)
        VALUES (
            {data['sk_chip']},
            {data['sk_aparelho']},
            '{data['tipo_movimento']}',
            '{data.get('origem','')}',
            '{data.get('observacao','')}',
            CURRENT_TIMESTAMP()
        )
    """

    bq.execute(sql)

    return jsonify({"status": "ok", "mensagem": "Movimentação registrada com sucesso!"})


# ===========================
# RODAR SERVIDOR
# ===========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
