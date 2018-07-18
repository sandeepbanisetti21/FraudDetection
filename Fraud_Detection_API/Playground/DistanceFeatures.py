import sys
from datetime import datetime
from math import radians, cos, sin, asin, sqrt

from bson.objectid import ObjectId

import DBController as dbc
import pandas as pd

import numpy as np
import matplotlib.pyplot as plt
import math


def createDistanceDataframe(resultSet):
    dfList = []
    for wave_id in resultSet:
        dataFrame = getDistanceDataFrameForWave(str(wave_id))
        dfList.append(dataFrame)
    project_distance_dataframe = pd.concat(dfList)

    project_distance_dataframe = cleanDataFrame(project_distance_dataframe)

    project_distance_dataframe['distance'] = project_distance_dataframe.apply(
        lambda x: haversine(x['data_item_latitude1'], x['data_item_longitude1'], x['location_latitude1'],
                            x['location_longitude']), axis=1)

    averageDataFrame = project_distance_dataframe.groupby('ticket', as_index=False).agg(
        {"distance": [("Avg Distance", "mean")]})

    maxDataFrame = project_distance_dataframe.groupby('ticket', as_index=False).agg(
        {"distance": [("Max Distance", "max")]})

    centroidDataFrame = project_distance_dataframe.groupby(['ticket', 'location_latitude1', 'location_longitude'],
                                                           as_index=False).agg(
        {'data_item_latitude1': ['mean'], 'data_item_longitude1': ['mean']})

    centroidDataFrame['centroid distance'] = centroidDataFrame.apply(
        lambda x: haversine(x['data_item_latitude1'], x['data_item_longitude1'], x['location_latitude1'],
                            x['location_longitude']), axis=1)

    zeroCountFrame = project_distance_dataframe.groupby('ticket', as_index=False).agg(
        {"data_item_latitude1": [("Zero-location-count", zeroCount)]})

    df = centroidDataFrame.merge(maxDataFrame, on='ticket').merge(averageDataFrame, on='ticket').merge(zeroCountFrame, on='ticket')

    df.to_csv('file.csv', index=False)

    return df


def cleanDataFrame(project_distance_dataframe):
    project_distance_dataframe['ticket'] = project_distance_dataframe.apply(lambda x: x['ticket_id'][0], axis=1)

    project_distance_dataframe['location_latitude1'] = project_distance_dataframe.apply(
        lambda x: x['location_latitude'][0], axis=1)

    project_distance_dataframe['data_item_latitude1'] = project_distance_dataframe.apply(
        lambda x: x['data_item_latitude'][0], axis=1)

    project_distance_dataframe['data_item_longitude1'] = project_distance_dataframe.apply(
        lambda x: x['data_item_longitude'][0], axis=1)

    project_distance_dataframe.drop(['data_item_latitude', 'data_item_longitude', 'location_latitude', 'ticket_id'],
                                    inplace=True, axis=1)

    return project_distance_dataframe


def customMean(x):
    list = x.tolist()
    locationList = []
    for m in list:
        if int(float(m)) is 0:
            continue
        locationList.append(float(m))
    return np.array(locationList).mean()

def zeroCount(series):
    counter = 0
    latitudeList = series.tolist()
    for latitude in latitudeList:
        if math.isnan(latitude):
            counter+=1
    return counter


def createTimeDataframe(resultSet):
    dfList = []
    for wave_id in resultSet:
        dataFrame = getTimeDataFrameForWave(str(wave_id))
        dfList.append(dataFrame)
    project_distance_dataframe = pd.concat(dfList)
    return project_distance_dataframe


def createEventDataframe(dataFrame):
    earliestStartDate = min(dataFrame['start_date'])
    latestEndDate = max(dataFrame['end_date'])

    print(earliestStartDate)
    print(latestEndDate)
    eventDateFrame = getAuditEventsDataFrame(earliestStartDate, latestEndDate)


def getDistinctData(organiztionSubscriptionId):
    getWavesForProjectQuery = 'select wave_id,start_date,end_date from organization_subscription_wave_stats where organization_subscription_id = %(orgid)s '
    resultSet = pd.read_sql_query(getWavesForProjectQuery, dbc.getPostgresSession(),
                                  params={'orgid': organiztionSubscriptionId})

    print(resultSet.to_string())

    waveIdList = resultSet['wave_id']
    distanceDataFrame = createDistanceDataframe(waveIdList)


def getDistanceDataFrameForWave(waveId):
    wave_match = {}
    project_match = {}
    wave_match['organization_subscription_waves.wave_id'] = ObjectId(waveId)
    project_match['ticket_id'] = "$ticket_id",
    project_match['data_item_latitude'] = "$data_item_latitude",
    project_match['data_item_longitude'] = "$data_item_longitude",
    project_match['location_latitude'] = "$location.latitude",
    project_match['location_longitude'] = "$location.longitude"

    df = pd.DataFrame(list(dbc.getMonogClient().data_items.aggregate(
        dbc.create_mongo_aggreagate_pipeline(match=wave_match, project=project_match))))

    return df

def getAuditEventsDataFrame(startTime, endTime):
    wave_match = {}
    timestampquery = {}
    timestampquery['$gte'] = datetime.fromtimestamp(startTime, None)
    timestampquery['lt'] = datetime.fromtimestamp(endTime, None)
    wave_match['timestamp'] = timestampquery

    group = {
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
            }
        }
    }
    project = {}
    project['authcount'] = "$authcount",
    project['workcount'] = "$workcount",
    project['customer_id'] = "$_id.source_customer_id",

    df = pd.DataFrame(list(dbc.getMonogClient().data_items.aggregate(
        dbc.create_mongo_aggreagate_pipeline(match=wave_match, project=project, group=group))))
    print(df.to_string())
    return df


def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lat2 is None or lon1 is None or lon2 is None:
        return 0

    # if int(float(lat1)) is 0 or int(float(lat2)) is 0 or int(float(lon1)) is 0 or int(float(lon2)) is 0:
    #    return 0

    R = 3959.87433  # this is in miles.  For Earth radius in kilometers use 6372.8 km

    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)

    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dLat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dLon / 2) ** 2
    c = 2 * asin(sqrt(a))

    return R * c


def main():
    dbc.configureDatabase()
    dataFrame = getDistinctData(sys.argv[1])


if __name__ == '__main__':
    main()
