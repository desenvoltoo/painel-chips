from .dashboard import bp_dashboard
from .chips import bp_chips
from .aparelhos import bp_aparelhos
from .recargas import bp_recargas
from .relacionamentos import bp_relacionamentos

def register_blueprints(app):
    app.register_blueprint(bp_dashboard)
    app.register_blueprint(bp_chips, url_prefix="/chips")
    app.register_blueprint(bp_aparelhos, url_prefix="/aparelhos")
    app.register_blueprint(bp_recargas, url_prefix="/recargas")
    app.register_blueprint(bp_relacionamentos, url_prefix="/relacionamentos")
