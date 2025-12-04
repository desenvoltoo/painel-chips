# routes/timeline.py

from flask import Blueprint, render_template, jsonify
from utils.bigquery_client import BigQueryClient
from utils.sanitizer import sanitize_df

timeline_bp = Blueprint("timeline", __name__)
bq = BigQueryClient()


# ============================================================
# TIMELINE DE UM CHIP ESPECÍFICO
# ============================================================
@timeline_bp.route("/chips/<int:sk_chip>/timeline")
def chip_timeline(sk_chip):

    try:
        df = sanitize_df(bq._run(f"""
            SELECT *
            FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
            WHERE sk_chip = {sk_chip}
            ORDER BY data_evento DESC
        """))

        eventos = df.to_dict(orient="records")

        return render_template("timeline.html", eventos=eventos)

    except Exception as e:
        print("🚨 Erro ao carregar timeline:", e)
        return "Erro ao carregar timeline", 500


# ============================================================
# API JSON (caso queira usar via AJAX)
# ============================================================
@timeline_bp.route("/api/chip/<int:sk_chip>/timeline")
def chip_timeline_api(sk_chip):

    df = bq._run(f"""
        SELECT *
        FROM `{bq.project}.{bq.dataset}.vw_chip_timeline`
        WHERE sk_chip = {sk_chip}
        ORDER BY data_evento DESC
    """)

    return jsonify(df.to_dict(orient="records"))
