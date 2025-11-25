# utils/relacionamentos.py
from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient

# Blueprint
relacionamentos_bp = Blueprint("relacionamentos", __name__)

# BigQuery Client
bq = BigQueryClient()


# =======================================================
# LISTAGEM DE RELACIONAMENTOS (FATO)
# =======================================================
@relacionamentos_bp.route("/relacionamentos")
def listar_relacionamentos():
    """
    Lista todos os vínculos entre chips e aparelhos
    (tabela f_chip_aparelho).
    """
    try:
        df = bq.get_eventos()

        # Converte para JSON
        relacionamentos = df.to_dict(orient="records")

        return render_template(
            "relacionamentos.html",
            relacionamentos=relacionamentos
        )

    except Exception as e:
        print("Erro ao carregar relacionamentos:", e)
        return "Erro ao carregar relacionamentos", 500


# =======================================================
# NOVO RELACIONAMENTO
# =======================================================
@relacionamentos_bp.route("/relacionamentos/add", methods=["POST"])
def adicionar_relacionamento():
    """
    Insere um novo relacionamento Chip → Aparelho
    (movimentação).
    """
    try:
        bq.insert_evento(request.form)
        return redirect("/relacionamentos")

    except Exception as e:
        print("Erro ao inserir relacionamento:", e)
        return "Erro ao inserir relacionamento", 500


# =======================================================
# DETALHE DO RELACIONAMENTO (opcional)
# =======================================================
@relacionamentos_bp.route("/relacionamentos/<id_row>")
def detalhe_relacionamento(id_row):
    """
    Busca 1 relacionamento específico.
    """
    try:
        df = bq.get_eventos()
        df = df[df["id"] == id_row]

        if len(df) == 0:
            return {"erro": "Relacionamento não encontrado"}, 404

        return df.to_dict(orient="records")[0]

    except Exception as e:
        print("Erro ao carregar registro:", e)
        return "Erro ao buscar relacionamento", 500
