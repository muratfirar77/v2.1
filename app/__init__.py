from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_jwt_extended import JWTManager
from flask_cors import CORS
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import timedelta

db = SQLAlchemy()
migrate = Migrate()
jwt = JWTManager()

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'your-default-flask-secret-key-123!@#')
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(app.instance_path, 'prototype.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'your-default-jwt-secret-key-xyz789!@#')
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(days=1)

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass 

    db.init_app(app)
    migrate.init_app(app, db)
    jwt.init_app(app)

    frontend_url = os.environ.get('FRONTEND_URL')
    if frontend_url:
        CORS(app, resources={r"/*": {"origins": frontend_url}}, supports_credentials=True)
    else:
        CORS(app, supports_credentials=True) # Geliştirme için

    from app import models 
    from app.routes import bp as main_bp
    app.register_blueprint(main_bp)
    
    if not app.debug and not app.testing:
        if not os.path.exists('logs'): os.mkdir('logs')
        file_handler = RotatingFileHandler('logs/risk_app.log', maxBytes=10240, backupCount=10, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
        app.logger.setLevel(logging.INFO)
        app.logger.info('Finansal Risk Uygulaması başlatılıyor (production)')
    else:
        app.logger.setLevel(logging.DEBUG)
        app.logger.info('Finansal Risk Uygulaması DEBUG modunda başlatılıyor')

    return app