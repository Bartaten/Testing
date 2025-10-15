from flask import Flask


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.update(
        SECRET_KEY="dev-secret-key",
        MAX_CONTENT_LENGTH=50 * 1024 * 1024,  # 50 MB
        UPLOAD_FOLDER="/workspace/app/uploads",
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=False,
    )

    # Register blueprints
    from .views import main_bp

    app.register_blueprint(main_bp)

    return app
