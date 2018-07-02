import os
import sys
import json,ast

from flask_api import FlaskAPI
from flask import request
from Fraud_Detection_API.FraudEngine import FraudModel
from Fraud_Detection_API.Util import ConfigurationParser
from Fraud_Detection_API.Util import Utility


class Config(object):
    DEBUG = os.getenv('DEBUG')
    CSRF_ENABLED = True
    POSTGRES_DATABASE_URI = os.getenv('POSTGRES_URL')
    MONGODB_DATABASE_URI = os.getenv('MONGODB_URL')


def createApp():
    app = FlaskAPI(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    ConfigurationParser.configure()

    @app.route('/bulk/user', methods=['POST'])
    def detectFraudUserLevel():
        try:
            return FraudModel.detectFraudUserLevel(Utility.getJsonAsDictionary(request.get_json()))
        except:
            print("Unexpected error:", sys.exc_info()[0])

    @app.route('/bulk/project', methods=['POST'])
    def detectFraudProjectLevel():
        try:
            return FraudModel.detectFraudProjectLevel(Utility.getJsonAsDictionary(request.get_json()))
        except:
            print("Unexpected error:", sys.exc_info()[0])

    @app.route('/user', methods=['POST'])
    def detectFraudSingleUser():
        try:
            return FraudModel.detectFraudSingleUser(Utility.getJsonAsDictionary(request.get_json()))
        except:
            print("Unexpected error:", sys.exc_info()[0])

    @app.route('/project', methods=['POST'])
    def detectFraudSingleProject():
        try:
            return FraudModel.detectFraudSingleProject(Utility.getJsonAsDictionary(request.get_json()))
        except:
            print("Unexpected error:", sys.exc_info()[0])

    @app.route('/org', methods=['POST'])
    def detectFraudOrgLvel():
        try:
            return FraudModel.detectFraudOrgLevel(Utility.getJsonAsDictionary(request.get_json()))
        except:
            print("Unexpected error:", sys.exc_info()[0])

    @app.route('/refreshView', methods=['POST'])
    def refreshMaterializedView():
        return FraudModel.refreshMaterializedView()

    return app

