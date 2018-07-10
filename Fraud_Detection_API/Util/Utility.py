import inspect
from psycopg2.extras import DictCursor
import json,ast
from math import sqrt

from Fraud_Detection_API.FraudEngine import Queries
import ConfigurationParser


def methodsWithDecorator(cls, decoratorName):
    methods = []
    sourcelines = inspect.getsourcelines(cls)[0]
    for i,line in enumerate(sourcelines):
        line = line.strip()
        if line.split('(')[0].strip() == '@'+decoratorName: # leaving a bit out
            nextLine = sourcelines[i+1]
            name = nextLine.split('def')[1].split('(')[0].strip()
            methods.append(name)
    return methods

def getDictCursor(connection):
    dict_cur = connection.cursor(cursor_factory=DictCursor)
    return dict_cur

def getCursor(connection):
    cursor = connection.cursor()
    return cursor

def getJsonAsDictionary(jsonData):
    jsonDataAsLiteral = ast.literal_eval(json.dumps(jsonData))
    return jsonDataAsLiteral


def getDistanceBetweenPoints(point1,point2):
    if point1[0] is None or point1[1] is None or point2[0] is None or point2[1] is None:
        return None

    vect_x = float(point2[0]) - float(point1[0])
    vect_y = float(point2[1]) - float(point1[1])
    return sqrt(vect_x ** 2 + vect_y ** 2)


def getOneUserData(id):
    print('inside one user data')
    dictCursor = getDictCursor(ConfigurationParser.config.getPostgresClient())
    dictCursor.execute(Queries.getSelectPgUserById(id))
    dataItem = dictCursor.fetchone()
    return dataItem

def getSingleProjectData(id):
    dictCursor = getDictCursor(ConfigurationParser.config.getPostgresClient())
    dictCursor.execute(Queries.getSelectPgUserById(id))
    dataItem = dictCursor.fetchone()
    return dataItem

def getTimeDifference(time1,time2):
    from datetime import datetime,date
    delta = time1 - time2
    return delta / 3600.0