# -*- coding: utf-8 -*-

import os
from flask import Flask

# Blueprints
from routes.aparelhos import aparelhos_bp
from routes.chips import chips_bp
from routes.recargas import recargas_bp
from routes.relacionamentos import relacionamentos_bp
from routes.movimentacao import mov_bp
from routes.dashboard import bp_dashboard   # 👈 DASHBOARD AQUI

# ================================
# CONFIGURAÇÃO GERAL
# ================================
PORT = int(os.getenv("PORT", 8080))

def create_app():
    app = Flask(__name__)

    # ================================
    # REGISTRO DOS BLUEPRINTS
    # ================================
    app.register_blueprint(bp_dashboard)     # /
    app.register_blueprint(aparelhos_bp)
    app.register_blueprint(chips_bp)
    app.register_blueprint(recargas_bp)
    app.register_blueprint(relacionamentos_bp)
    app.register_blueprint(mov_bp)

    return app

app = create_app()

# ================================
# RUN SERVER
# ================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
