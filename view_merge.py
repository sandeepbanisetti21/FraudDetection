import psycopg2
from pymongo import MongoClient
import csv
import os


def getPostgresCursor():
    conn = psycopg2.connect(host="localhost", database="gigwalk_test", user="sandeep", password="Aspirin1@")
    return conn

def getMongoClient():
    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client['test']
    return db

def getAbsolutePath():
    cwd = os.getcwd();
    return cwd

def executePostgresQuery(postgresConnection, query):
    postgresConnection.cursor().execute(query)
    postgresConnection.commit()

def createCSVFromMongoView(mongodbClient):
    mongoView = mongodbClient.mongo_user_view
    events = mongoView.find()
    with open('test.csv', 'w') as outfile:
        fields = ['_id', 'authcount', 'workcount', 'customer_id', 'lastaudittime', 'lastLocation']
        write = csv.DictWriter(outfile, fieldnames=fields)
        # write.writeheader()
        for row in events:
            write.writerow(row)

def mergeViews(postgresConnection, mongodbClient):
    query = 'CREATE TABLE mongo_user_view(_id varchar, authcount varchar, workcount varchar,customer_id bigint primary key, lastaudittime timestamp, lastlocation varchar)'
    executePostgresQuery(postgresConnection, query)
    createCSVFromMongoView(mongodbClient)
    copyQuery = "COPY mongo_user_view FROM '" + getAbsolutePath() + "/test.csv' WITH (FORMAT csv);"
    executePostgresQuery(postgresConnection, copyQuery)
    viewQuery = 'CREATE MATERIALIZED VIEW user_stats_view AS SELECT a.*,b.* from pg_user_view as a left join (select authcount,workcount,customer_id.lastaudittime,lastlocation from mongo_user_view) b on b.customer_id = a.id'
    executePostgresQuery(postgresConnection,viewQuery)

def main():
    pgConn = getPostgresCursor()
    mongoDbClient = getMongoClient()
    mergeViews(pgConn, mongoDbClient)


if __name__ == '__main__':
    main()
