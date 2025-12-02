# -*- coding: utf-8 -*-

# ============================
# IMPORTAÇÃO DOS BLUEPRINTS
# ============================

from .chips import chips_bp
from .aparelhos import aparelhos_bp
from .recargas import recargas_bp
from .relacionamentos import relacionamentos_bp
from .dashboard import bp_dashboard



# ============================
# REGISTRO DOS BLUEPRINTS
# ============================

def register_blueprints(app):


    # Dashboard geral
    app.register_blueprint(bp_dashboard)

    # Módulos operacionais
    app.register_blueprint(chips_bp)
    app.register_blueprint(aparelhos_bp)
    app.register_blueprint(recargas_bp)
    app.register_blueprint(relacionamentos_bp)

