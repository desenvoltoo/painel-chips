# -*- coding: utf-8 -*-
import os
from flask import Flask

# Blueprints
from routes.aparelhos import aparelhos_bp
from routes.chips import chips_bp
from routes.recargas import recargas_bp
from routes.relacionamentos import relacionamentos_bp
from routes.movimentacao import mov_bp
from routes.dashboard import bp_dashboard


# ================================
# CONFIG
# ================================
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


PORT = _env_int("PORT", 8080)


def create_app() -> Flask:
    # Se seus templates/static estão no padrão (templates/ e static/),
    # não precisa passar template_folder/static_folder.
    app = Flask(__name__)

    # Segurança mínima (evita erro em session/flash)
    app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-change-me")

    # ================================
    # ROTAS DE SAÚDE (útil no Cloud Run)
    # ================================
    @app.get("/health")
    def health():
        return {"ok": True}, 200

    # ================================ 
    # BLUEPRINTS
    # ================================
    # dashboard em "/"
    app.register_blueprint(bp_dashboard)

    # demais módulos (se quiser prefixos depois, é aqui)
    app.register_blueprint(aparelhos_bp)
    app.register_blueprint(chips_bp)
    app.register_blueprint(recargas_bp)
    app.register_blueprint(relacionamentos_bp)
    app.register_blueprint(mov_bp)

    return app


app = create_app()


if __name__ == "__main__":
    # Debug opcional via env
    debug = os.getenv("FLASK_DEBUG", "0").strip() == "1"
    app.run(host="0.0.0.0", port=PORT, debug=debug)
