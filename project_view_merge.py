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
    mongoView = mongodbClient.data_items
    pipeline = [{
            "$sort":
                {
                    "ticket_id": 1
                }
        },
        {
            "$group": {
                "_id": {
                    "ticket_id": "$ticket_id"
                },
                "data_item_count": {
                    "$sum": 1
                },
                "latitude": {
                    "$last": "$data_item_latitude"
                },
                "longitude": {
                    "$last": "$data_item_longitude"
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "data_item_count": "$data_item_count",
                "latitude": "$latitude",
                "ticket_id": "$_id.ticket_id",
                "longitude": "$longitude"
            }
        },
        {
            "$sort" : {
                "ticket_id" : 1
            }
        }
    ]
    eventCursor = mongoView.aggregate(pipeline)
    events = list(eventCursor)
    #print(events)
    with open('test_project.csv', 'w') as outfile:
        fields = ['_id', 'data_item_count', 'latitude', 'ticket_id', 'longitude']
        write = csv.DictWriter(outfile, fieldnames=fields)
        # write.writeheader()
        for row in events:
            write.writerow(row)


def mergeViews(postgresConnection, mongodbClient):
    query = 'CREATE TABLE mongo_project_view(_id varchar, data_item_count varchar, latitude varchar,  ticket_id bigint primary key,longitude varchar)'
    executePostgresQuery(postgresConnection, query)
    createCSVFromMongoView(mongodbClient)
    copyQuery = "COPY mongo_project_view FROM STDIN DELIMITER ',' CSV"
    with open(getAbsolutePath() + '/test_project.csv') as f:
        postgresConnection.cursor().copy_expert(copyQuery, f)
    postgresConnection.commit()
    viewQuery = 'CREATE MATERIALIZED VIEW project_stats_view AS select a.*,b.data_item_count,b.latitude,b.ticket_id,b.longitude from pg_project_view as a left join (select data_item_count,latitude,ticket_id,longitude from mongo_project_view) b on a.id = b.ticket_id'
    executePostgresQuery(postgresConnection,viewQuery)


def main():
    pgConn = getPostgresCursor()
    mongoDbClient = getMongoClient()
    mergeViews(pgConn, mongoDbClient)


if __name__ == '__main__':
    main()
