import psycopg2
import ConfigurationParser as cp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient

def getPostgresClient():
    print('created postgres client')
    print(cp.postgresUrl)
    conn = psycopg2.connect(cp.postgresUrl)
    return conn

def getSqlAlchemySession():
    print('created  sql alchemy session')
    sqlalchemyEngine =  create_engine(cp.postgresUrl)
    return sqlalchemyEngine

def getMongoDbClient():
    print('created MongoDbClient')
    client = MongoClient(cp.mongoDbUrl)
    return client

