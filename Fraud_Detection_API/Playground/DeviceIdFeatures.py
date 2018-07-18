import sys

from bson.objectid import ObjectId

import DBController as dbc
import pandas as pd

import matplotlib.pyplot as plt

def createDeviceDataframe(waveIdlist):
    dfList = []
    for wave_id in waveIdlist:
        dataFrame = getDeviceDataset(str(wave_id))
        dfList.append(dataFrame)
    project_device_data_dataframe = pd.concat(dfList)

    return project_device_data_dataframe


def getDistinctDeviceData(organiztionSubscriptionId):
    getWavesForProjectQuery = 'select wave_id,start_date,end_date from organization_subscription_wave_stats where organization_subscription_id = %(orgid)s '
    resultSet = pd.read_sql_query(getWavesForProjectQuery, dbc.getPostgresSession(),
                                  params={'orgid': organiztionSubscriptionId})

    deviceDataframe = createDeviceDataframe(resultSet['wave_id'])
    processDataFrame(deviceDataframe)


def processDataFrame(dataFrame):
    #un comment this while connecting to db
    #cleanDataFrame(dataFrame)
    dataFrame.drop(['_id'], inplace=True, axis=1)

    totalDataItems = dataFrame.groupby(['ticket_id'], as_index=False).agg([("Data item count", 'count')])

    result = dataFrame.groupby(['ticket_id','device_id'], as_index=True)['device_id'].agg(['count']).reset_index()
    totalDevices = result.groupby(['ticket_id'], as_index=False).agg([("Device count", 'count')])


    deviceid_df = totalDevices.merge(totalDataItems, on='ticket_id').reset_index()

    deviceid_df.drop(['count'], inplace=True, axis=1)

    #plt.scatter(deviceid_df['ticket_id'], deviceid_df[('device_id', 'Device count')])
    #plt.show()  # Depending on whether you use IPython or interactive mode, etc.
    print(deviceid_df)
    return deviceid_df

def cleanDataFrame(dataframe):
    dataframe['ticket_id'] = dataframe.apply(
        lambda x: x['ticket_id'][0], axis=1)

    return dataframe

def getDeviceDataset(waveId):
    wave_match = {}
    project_match = {}
    wave_match['organization_subscription_waves.wave_id'] = ObjectId(waveId)
    project_match['ticket_id'] = "$ticket_id",
    project_match['device_id'] = "$device_id"

    df = pd.DataFrame(list(dbc.getMonogClient().data_items.aggregate(
        dbc.create_mongo_aggreagate_pipeline(match=wave_match, project=project_match))))



    return df



def main():
    #dbc.configureDatabase()
    #timestampDataframe = getDistinctDeviceData(sys.argv[1])
    df = pd.read_csv('file_deviceid.csv')
    processDataFrame(df)


if __name__ == '__main__':
    main()