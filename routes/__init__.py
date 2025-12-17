# routes/__init__.py
# -*- coding: utf-8 -*-

def register_blueprints(app):
    # IMPORTA AQUI DENTRO (lazy import) PRA EVITAR CIRCULAR IMPORT
    from .dashboard import bp_dashboard
    from .aparelhos import aparelhos_bp
    from .chips import chips_bp
    from .relacionamentos import relacionamentos_bp
    from .movimentacao import mov_bp  # se você tiver

    app.register_blueprint(bp_dashboard)
    app.register_blueprint(aparelhos_bp)
    app.register_blueprint(chips_bp)
    app.register_blueprint(relacionamentos_bp)
    app.register_blueprint(mov_bp)
