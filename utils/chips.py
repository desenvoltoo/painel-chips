# utils/chips.py
from flask import Blueprint, render_template, request, redirect
from utils.bigquery_client import BigQueryClient

bq = BigQueryClient()
chips_bp = Blueprint("chips", __name__)


# =======================================================
# LISTAGEM DE CHIPS
# =======================================================
@chips_bp.route("/chips")
def chips_list():
    chips_df = bq.get_chips()
    aparelhos_df = bq.get_aparelhos()

    # === SANITIZA PARA EVITAR ERRO NA TABELA ===
    from app import sanitize_df
    chips_df = sanitize_df(chips_df)
    aparelhos_df = sanitize_df(aparelhos_df)

    return render_template(
        "chips.html",
        chips=chips_df.to_dict(orient="records"),
        aparelhos=aparelhos_df.to_dict(orient="records")
    )

# =======================================================
# UPSERT (INSERIR + EDITAR)
# =======================================================
@chips_bp.route("/chips/add", methods=["POST"])
def chips_add():
    bq.upsert_chip(request.form)
    return redirect("/chips")


# =======================================================
# GET (APENAS UM CHIP — se você quiser depois)
# =======================================================
@chips_bp.route("/chips/<id_chip>")
def get_chip(id_chip):
    df = bq.get_chips()
    df = df[df["id_chip"] == id_chip]

    if len(df) == 0:
        return {"erro": "Chip não encontrado"}, 404

    return df.to_dict(orient="records")[0]

# =======================================================
# EDIT VIEW (carrega formulário com dados preenchidos)
# =======================================================
@chips_bp.route("/chips/edit/<id_chip>")
def get_chips(self):
    sql = f"""
        SELECT
            sk_chip,
            id_chip,
            numero,
            operadora,
            plano,
            status,
            dt_inicio,
            ultima_recarga_valor,
            ultima_recarga_data,
            total_gasto,
            sk_aparelho_atual,
            ativo
        FROM `{PROJECT}.{DATASET}.dim_chip`
        ORDER BY numero
    """
    return self._run(sql)

# =======================================================
# MOVIMENTAÇÃO DE CHIP (INSTALAR / REMOVER / TROCA)
# =======================================================
@chips_bp.route("/chips/movimento", methods=["POST"])
def chips_movimento():
    try:
        dados = request.json
        
        sk_chip = dados.get("sk_chip")
        sk_aparelho = dados.get("sk_aparelho")  # pode ser NULL
        tipo = dados.get("tipo")
        origem = dados.get("origem", "Painel")
        observacao = dados.get("observacao", "")

        ok = bq.registrar_movimento_chip(
            sk_chip=sk_chip,
            sk_aparelho=sk_aparelho,
            tipo=tipo,
            origem=origem,
            observacao=observacao
        )

        return {"success": ok}

    except Exception as e:
        print("Erro movimento chip:", e)
        return {"success": False, "erro": str(e)}, 500


