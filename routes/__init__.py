# routes/__init__.py

from .dashboard import bp_dashboard
from .chips import chips_bp
from .aparelhos import bp_aparelhos
from .recargas import recargas_bp
from .relacionamentos import relacionamentos_bp


def register_blueprints(app):
    app.register_blueprint(bp_dashboard)
    app.register_blueprint(chips_bp)
    app.register_blueprint(bp_aparelhos)
    app.register_blueprint(recargas_bp)
    app.register_blueprint(relacionamentos_bp)
