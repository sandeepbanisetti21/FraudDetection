import os

from flask_api import FlaskAPI
from Fraud_Detection_API.FraudEngine import FraudModel


class Config(object):
    DEBUG = os.getenv('DEBUG')
    CSRF_ENABLED = True
    POSTGRES_DATABASE_URI = os.getenv('POSTGRES_URL')
    MONGODB_DATABASE_URI = os.getenv('MONGODB_URL')


def createApp():
    app = FlaskAPI(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    app.config.from_pyfile('__init__.py')

    @app.route('/bulk/user', methods=['GET'])
    def detectFraudUserLevel():
        return FraudModel.detectFraudUserLevel()

    @app.route('/bulk/project', methods=['GET'])
    def detectFraudProjectLevel():
        return FraudModel.detectFraudProjectLevel()

    @app.route('/refreshView', methods=['GET'])
    def refreshMaterializedView():
        return FraudModel.refreshMaterializedView()

    return app

