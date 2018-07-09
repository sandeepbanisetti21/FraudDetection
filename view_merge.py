import psycopg2
from pymongo import MongoClient
import csv
import os


def getPostgresCursor():
    # conn = psycopg2.connect(host="localhost", database="gigwalk_test", user="sandeep", password="Aspirin1@")
    conn = psycopg2.connect(
        'postgres://alembic:W5hYt8AoF1FnBwGaElU2B@fraudtest.cgxxy7nmhwzi.us-east-1.rds.amazonaws.com:5432/gw_apps_1')
    return conn


def getMongoClient():
    # client = MongoClient('mongodb://127.0.0.1:27017/')
    client = MongoClient(
        'mongodb://fraudtest:Xg6ZOTsHvjsGKvEu8NNh@ds117051-a0.mlab.com:17051,ds117051-a1.mlab.com:17051/gigwalk_apps_1')
    mongoClient2 = client['gigwalk_apps_1']
    return mongoClient2


def getAbsolutePath():
    cwd = os.getcwd();
    return cwd


def executePostgresQuery(postgresConnection, query):
    postgresConnection.cursor().execute(query)
    postgresConnection.commit()


def createCSVFromMongoView(mongodbClient):
    mongoView = mongodbClient.audit_events
    pipeline = [
        {
            "$sort":
                {
                    "timestamp": 1
                }
        },
        {
            "$group": {
                "_id": {
                    "source_customer_id": "$source_customer_id"
                },
                "authcount": {
                    "$sum": {
                        '$cond': [{'$eq': ['$audit_type', 'USER_AUTH']}, 1, 0]
                    }
                },
                "workcount": {
                    "$sum": {
                        '$cond': [{'$eq': ['$audit_type', 'LOOKING_FOR_WORK']}, 1, 0]
                    }
                },
                "lastauditTime": {
                    "$last": "$timestamp"
                },
                "lastLocation": {
                    "$last": "$context"
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "authcount": "$authcount",
                "workcount": "$workcount",
                "customer_id": "$_id.source_customer_id",
                "lastaudittime": "$lastauditTime",
                "lastLocation": "$lastLocation"
            }
        },
        {
            "$sort": {
                "customer_id": 1
            }
        }
    ]
    eventCursor = mongoView.aggregate(pipeline)
    events = list(eventCursor)
    # print(events)
    with open('test2.csv', 'w') as outfile:
        fields = ['_id', 'authcount', 'workcount', 'customer_id', 'lastaudittime', 'lastLocation']
        write = csv.DictWriter(outfile, fieldnames=fields)
        # write.writeheader()
        for row in events:
            write.writerow(row)


def mergeViews(postgresConnection, mongodbClient):
    query = 'CREATE TABLE mongo_user_view(_id varchar, authcount varchar, workcount varchar,customer_id bigint primary key, lastaudittime timestamp, lastlocation varchar)'
    executePostgresQuery(postgresConnection, query)
    createCSVFromMongoView(mongodbClient)
    copyQuery = "COPY mongo_user_view FROM STDIN DELIMITER ',' CSV"
    with open(getAbsolutePath() + '/test2.csv') as f:
        postgresConnection.cursor().copy_expert(copyQuery, f)
    postgresConnection.commit()
    viewQuery = 'CREATE MATERIALIZED VIEW user_stats_view AS select a.*,b.authcount,b.workcount,b.customer_id,b.lastaudittime,b.lastlocation from pg_user_view as a left join (select authcount,workcount,customer_id,lastaudittime,lastlocation from mongo_user_view) b on a.id = b.customer_id'
    executePostgresQuery(postgresConnection,viewQuery)


def main():
    pgConn = getPostgresCursor()
    mongoDbClient = getMongoClient()
    mergeViews(pgConn, mongoDbClient)


if __name__ == '__main__':
    main()
