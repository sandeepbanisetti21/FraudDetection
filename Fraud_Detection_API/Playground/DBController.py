import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import os

configuration = dict()

POSTGRES_CLIENT = 'postgres_client'
MONGO_CLINET = 'mongo_client'
SQLALCHEMY_SESSION = 'sqlalchemy_session'

postgresUrl = os.environ.get("POSTGRES_URL")
mongoDbUrl = os.environ.get("MONGO_DB_URL")


def create_mongo_aggreagate_pipeline(match=None,
                                     project=None,
                                     group=None):
    aggregate = []
    if match is not None:
        aggregate.append({'$match': match})
    if project is not None:
        aggregate.append({'$project': project})
    if group is not None:
        aggregate.append({'$group': group})
    return aggregate


def executePostgresQuery(query, parameters):
    engine = configuration[SQLALCHEMY_SESSION]
    resultSet = engine.execute(query, parameters).fetchall()
    return resultSet


def configureDatabase():
    global configuration
    #configuration[POSTGRES_CLIENT] = configurePostgres()
    configuration[MONGO_CLINET] = configureMongo()
    configuration[SQLALCHEMY_SESSION] = configureSqlAlchemySession()


def configurePostgres():
    print('created postgres client')
    print(postgresUrl)
    conn = psycopg2.connect(postgresUrl)
    return conn

def getMonogClient():
    return configuration[MONGO_CLINET]


def getPostgresSession():
    return configuration[SQLALCHEMY_SESSION]




def configureMongo():
    print('created MongoDbClient')
    client = MongoClient(mongoDbUrl)
    mongoClient = client['gigwalk_apps_1']
    return mongoClient


def configureSqlAlchemySession():
    print('created  sql alchemy session')
    sqlalchemyEngine = create_engine(postgresUrl)
    return sqlalchemyEngine
