# -*- coding: utf-8 -*-

# ============================
# IMPORTAÇÃO DOS BLUEPRINTS
# ============================

from .auth import auth_bp          #  <-- FALTAVA
from .chips import chips_bp
from .aparelhos import aparelhos_bp
from .recargas import recargas_bp
from .relacionamentos import relacionamentos_bp
from .dashboard import bp_dashboard
from .admin import admin_bp


# ============================
# REGISTRO DOS BLUEPRINTS
# ============================

def register_blueprints(app):

    # Autenticação (login/logout)
    app.register_blueprint(auth_bp)   # <-- ESSÊNCIA DO LOGIN FUNCIONAR

    # Painel administrativo
    app.register_blueprint(admin_bp)

    # Dashboard geral
    app.register_blueprint(bp_dashboard)

    # Módulos operacionais
    app.register_blueprint(chips_bp)
    app.register_blueprint(aparelhos_bp)
    app.register_blueprint(recargas_bp)
    app.register_blueprint(relacionamentos_bp)
