# utils/relacionamentos.py
from flask import Blueprint, render_template, request, redirect, jsonify
from utils.bigquery_client import BigQueryClient
from app import sanitize_df

relacionamentos_bp = Blueprint("relacionamentos", __name__)
bq = BigQueryClient()


# =======================================================
# LISTAGEM DE RELACIONAMENTOS
# =======================================================
@relacionamentos_bp.route("/relacionamentos")
def listar_relacionamentos():
    try:
        df = bq.get_eventos()
        df = sanitize_df(df)

        return render_template(
            "relacionamentos.html",
            relacionamentos=df.to_dict(orient="records")
        )

    except Exception as e:
        print("Erro ao carregar relacionamentos:", e)
        return "Erro ao carregar relacionamentos", 500


# =======================================================
# INSERIR NOVO RELACIONAMENTO (MOVIMENTAÇÃO)
# =======================================================
@relacionamentos_bp.route("/relacionamentos/add", methods=["POST"])
def adicionar_relacionamento():
    try:
        dados = request.form

        bq.registrar_movimento_chip(
            sk_chip=dados.get("sk_chip"),
            sk_aparelho=dados.get("sk_aparelho"),
            tipo=dados.get("tipo"),
            origem=dados.get("origem", "Painel"),
            observacao=dados.get("observacao", "")
        )

        return redirect("/relacionamentos")

    except Exception as e:
        print("Erro ao inserir relacionamento:", e)
        return "Erro ao inserir relacionamento", 500


# =======================================================
# DETALHE DE UM RELACIONAMENTO
# =======================================================
@relacionamentos_bp.route("/relacionamentos/<sk_fato>")
def detalhe_relacionamento(sk_fato):
    try:
        df = bq.get_eventos()
        df = df[df["sk_fato"] == int(sk_fato)]

        if df.empty:
            return jsonify({"erro": "Relacionamento não encontrado"}), 404

        return jsonify(df.to_dict(orient="records")[0])

    except Exception as e:
        print("Erro ao carregar registro:", e)
        return "Erro ao buscar relacionamento", 500
