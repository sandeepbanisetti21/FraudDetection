from sqlalchemy import create_engine
import os
import pandas as pd

from math import radians, cos, sin, asin, sqrt
import matplotlib.pyplot as plt


def getPostgresEngine():
    engine = create_engine(os.environ.get("POSTGRES_URL"))
    return engine


def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lat2 is None or lon1 is None or lon2 is None:
        return 0

    if int(float(lat1)) is 0 or int(float(lat2)) is 0 or int(float(lon1)) is 0 or int(float(lon2)) is 0:
        return 0

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


def modifyDataframe(dataframe):
    dataframe['index1'] = dataframe.index

    dataframe['distance'] = dataframe.apply(
        lambda x: haversine(x['ticket_latitude'], x['ticket_longitude'], x['latitude'],
                            x['longitude']), axis=1)

    return dataframe


def main():
    engine = getPostgresEngine()
    selectPgQuery = "select * from project_stats_view where organization_subscription_id = '12048993' and status = 'SUBMITTED'"
    dataframe = pd.read_sql_query(selectPgQuery, con=engine)
    dataframe = modifyDataframe(dataframe)

    print(dataframe.to_string())
    plt.scatter(dataframe['index1'], dataframe['distance'])
    plt.show()  # Depending on whether you use IPython or interactive mode, etc.


if __name__ == '__main__':
    main()
