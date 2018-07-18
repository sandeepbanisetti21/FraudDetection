import pandas as pd
from bson.objectid import ObjectId
from pytz import timezone, common_timezones
import DBController as dbc
import sys
import pytz
from datetime import datetime
import matplotlib.pyplot as plt


def createTimeDataframe(waveIdlist):
    dfList = []
    for wave_id in waveIdlist:
        dataFrame = getTimeDataFrameForWave(str(wave_id))
        dfList.append(dataFrame)
    project_time_dataframe = pd.concat(dfList)
    project_time_dataframe = cleanData(project_time_dataframe)

    return project_time_dataframe


def getDistinctData(organiztionSubscriptionId):
    getWavesForProjectQuery = 'select wave_id,start_date,end_date from organization_subscription_wave_stats where organization_subscription_id = %(orgid)s '
    resultSet = pd.read_sql_query(getWavesForProjectQuery, dbc.getPostgresSession(),
                                  params={'orgid': organiztionSubscriptionId})
    timezoneQuery = 'select a.id,b.tzid from tickets a, locations b where a.organization_subscription_id = %(orgid)s and a.location_id = b.id'
    timezone_df = pd.read_sql_query(timezoneQuery, dbc.getPostgresSession(),
                                    params={'orgid': organiztionSubscriptionId})

    timeDataframe = createTimeDataframe(resultSet['wave_id'])
    analyzeTimeData(timeDataframe, timezone_df)


def queryRowfromDataframe(dataframe, columnName, value):
    return dataframe.loc[dataframe[columnName] == value]


def convert_datetime_timezone(input, destination_timezone, source_timezone='UTC'):
    input_datetime = pd.Timestamp(input).to_pydatetime()
    src_tz = timezone(source_timezone)
    dst_tz = timezone(destination_timezone)

    src_date = src_tz.localize(input_datetime)

    final_datetime = src_date.astimezone(dst_tz)
    final_datetime = datetime(final_datetime.year, final_datetime.month, final_datetime.day,
                              final_datetime.hour, final_datetime.minute, final_datetime.second)

    return final_datetime


def analyzeTimeData(timestampdata, timezonedata):
    result = timestampdata.groupby('ticket_id', as_index=False)['data_item_timestamp'].agg(['max', 'min', 'count'])
    result['duration'] = result['max'] - result['min']

    timestampdata['timezone_timestamp'] = timestampdata.apply(
        lambda x: convert_datetime_timezone(input=x['data_item_timestamp'], destination_timezone=
        queryRowfromDataframe(timezonedata, 'id', x['ticket_id'])['tzid'].iloc[0]), axis=1)

    timestampdata['timezone_timestamp'] = pd.to_datetime(timestampdata['timezone_timestamp'])
    timestampdata['time'] = timestampdata['timezone_timestamp'].dt.time

    offcountDataFrame = timestampdata.groupby('ticket_id', as_index=False).agg(
        {"timezone_timestamp": [("Off hour count", offhourscount)]})

    timestamp_df = result.merge(offcountDataFrame, on='ticket_id')

    print(timestamp_df)
    
    #plt.scatter(offcountDataFrame['ticket_id'], offcountDataFrame['timezone_timestamp'])
    #plt.show()  # Depending on whether you use IPython or interactive mode, etc.

    #ax = (timestampdata['timezone_timestamp'].groupby(timestampdata['timezone_timestamp'].dt.hour).count()).plot(kind="bar", color='#494949')
    #plt.show()

    # timestampdata['timezone'] = timestampdata.apply(
    #    lambda x: queryRowfromDataframe(timezonedata, 'id', x['ticket_id'])['tzid'].iloc[0], axis=1)



def offhourscount(series):
    counter = 0
    timestampList = series.tolist()
    for timestamp in timestampList:
        hour = pd.to_datetime(timestamp).hour
        if hour < 6 or hour > 21:
            counter+=1
    return counter

def getTimeDataFrameForWave(waveId):
    wave_match = {}
    project_match = {}
    wave_match['organization_subscription_waves.wave_id'] = ObjectId(waveId)
    project_match['ticket_id'] = "$ticket_id",
    project_match['data_item_timestamp'] = "$data_item_timestamp",

    df = pd.DataFrame(list(dbc.getMonogClient().data_items.aggregate(
        dbc.create_mongo_aggreagate_pipeline(match=wave_match, project=project_match))))

    return df


def cleanData(dataframe):
    dataframe['data_item_timestamp'] = dataframe.apply(
        lambda x: x['data_item_timestamp'][0], axis=1)

    dataframe['ticket_id'] = dataframe.apply(
        lambda x: x['ticket_id'][0], axis=1)

    dataframe.drop(['_id'], inplace=True, axis=1)

    return dataframe


def main():
    dbc.configureDatabase()
    timestampDataframe = getDistinctData(sys.argv[1])
    # print(convert_datetime_timezone(input='2015-06-29 22:07:38.599',destination_timezone="America/New_York"))


if __name__ == '__main__':
    main()
