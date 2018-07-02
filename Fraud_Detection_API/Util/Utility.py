import inspect
from psycopg2.extras import DictCursor
import json,ast

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