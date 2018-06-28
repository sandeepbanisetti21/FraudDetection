import psycopg2
from collections import  defaultdict
from sklearn.cluster import DBSCAN
from geopy.distance import great_circle
from shapely.geometry import MultiPoint
import numpy as np
import pandas as pd


def getdbConnection():
    conn = psycopg2.connect(host="localhost", database="gigwalk_test", user="sandeep", password="Aspirin1@")
    return conn.cursor()

def getCustomerIds(cursor):
    query = 'select id from user_view'
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

def getClusterInfo(customerIds,db):
    clusterData = defaultdict()
    for x in customerIds:
        query = "select geom_pin,geog_pin from locations where id in (Select location_id from tickets where assigned_customer_id = )"
        db.execute(query)
        rows = db.fetchall()
        loc = []
        for geom_pin,geog_pin in rows:
            loc.append((geog_pin,geom_pin))
        numofClusters,clusterGeom = clusterize(loc)
        clusterData[x] = [numofClusters,clusterGeom]
    return clusterData

def clusterize(loc):
    coords = loc.as_matrix(columns=['lat', 'lon'])
    kms_per_radian = 6371.0088
    epsilon = 1.5 / kms_per_radian
    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    num_clusters = len(set(cluster_labels))
    clusters = pd.Series([coords[cluster_labels == n] for n in range(num_clusters)])
    centermost_points = clusters.map(get_centermost_point)
    return clusters,centermost_points

def get_centermost_point(cluster):
    centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
    centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
    return tuple(centermost_point)


def updateView(db,clusterInfo):
    query = 'update user_view set clusters = ? and geom = ? where id = ?'
    for x in clusterInfo:
        query.set(1,x)
        query.set(2,clusterInfo[x][0])
        query.set(3, clusterInfo[x][1])
        db.execute(query)

def main():
    db = getdbConnection()
    customerIds = getCustomerIds(db)
    clusterInfo = getClusterInfo(customerIds,db)
    updateView(db,clusterInfo)
    pass

if __name__ == '__main__':
    main()
