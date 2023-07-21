import pandas as pd
from datetime import datetime
import numpy as np


import h3
import h3.api.numpy_int as h3int
import pyspark.sql.functions as F

from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import NullType
from pyspark.sql.functions import pandas_udf, PandasUDFType, col,to_timestamp
 #allow multiple outputs in one jupyter cell
from IPython.core.interactiveshell import InteractiveShell 
InteractiveShell.ast_node_interactivity = "all"


pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.options.mode.chained_assignment = None  # default='warn'

from IPython.display import HTML
from IPython.display import display
import base64  

import json
import pandas as pd
from io import StringIO
from urllib import parse
import folium
from folium import plugins
from folium.plugins import TimestampedGeoJson
import requests

def visualize_hexagons(hexagons, color="red", folium_map=None):
    """
    hexagons is a list of hexcluster. Each hexcluster is a list of hexagons. 
    eg. [[hex1, hex2], [hex3, hex4]]
    """
    polylines = []
    lat = []
    lng = []
    for hex in hexagons:
        polygons = h3.h3_set_to_multi_polygon([hex], geo_json=False)
        # flatten polygons into loops.
        outlines = [loop for polygon in polygons for loop in polygon]
        polyline = [outline + [outline[0]] for outline in outlines][0]
        lat.extend(map(lambda v:v[0],polyline))
        lng.extend(map(lambda v:v[1],polyline))
        polylines.append(polyline)
    
    if folium_map is None:
        m = folium.Map(location=[sum(lat)/len(lat), sum(lng)/len(lng)], zoom_start=16, tiles='cartodbpositron')
    else:
        m = folium_map
    for polyline in polylines:
        my_PolyLine=folium.PolyLine(locations=polyline,weight=8,color=color)
        m.add_child(my_PolyLine)
    return m

def visualize_hexagonsDF(hexagons,hexagons_field, hexagons_label,color="red", folium_map=None):
    """
    hexagons is a list of hexcluster. Each hexcluster is a list of hexagons. 
    eg. [[hex1, hex2], [hex3, hex4]]
    """
    polylines = []
    labels = []
    lat = []
    lng = []
    for index,row in hexagons.iterrows():
        hex=row[hexagons_field]
        label=str(row[hexagons_label])+':'+str(row[hexagons_field])
        labels.append(label)
        polygons = h3.h3_set_to_multi_polygon([hex], geo_json=False)
        # flatten polygons into loops.
        outlines = [loop for polygon in polygons for loop in polygon]
        polyline = [outline + [outline[0]] for outline in outlines][0]
        lat.extend(map(lambda v:v[0],polyline))
        lng.extend(map(lambda v:v[1],polyline))
        polylines.append(polyline)
    
    if folium_map is None:
        m = folium.Map(location=[sum(lat)/len(lat), sum(lng)/len(lng)], zoom_start=16, tiles='cartodbpositron')
    else:
        m = folium_map
    for index,polyline in enumerate(polylines):
        my_PolyLine=folium.PolyLine(locations=polyline,tooltip=labels[index],weight=8,color=color,popup=labels[index])
      
        m.add_child(my_PolyLine)
    return m

def displayRouteNoPort(pd_df,    start_date_filter: datetime=None, end_date_filter: datetime = None):
    if start_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])>=start_date_filter]
    if end_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])<end_date_filter]
     
    pdmmsi_set=pd_df.sort_values(['imo','dt_pos_utc'], ascending=[True, True] )
    len(pdmmsi_set)
    m = folium.Map(location=[42.092422, 11.795413])
    loc_red=[]
    loc_green=[]
    for index,coord in pdmmsi_set.iterrows():
        if coord['sog']>0.1:
            loc_green.append(( coord['latitude'], coord['longitude']))
        else:
            loc_red.append(( coord['latitude'], coord['longitude']))


    for coord in loc_red:
            folium.Marker( location=[ coord[0], coord[1] ], fill_color='#43d9de', radius=8 ).add_to( m )
    folium.PolyLine(loc_green,
                color='green',
                weight=5,
                opacity=0.8).add_to(m)
    display(m) 

def trackRoute(pd_df,    start_date_filter: datetime=None, end_date_filter: datetime = None,res=8):
    if start_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])>=start_date_filter]
    if end_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])<end_date_filter]
     
    sp=pd_df.sort_values(['imo','dt_pos_utc'], ascending=[True, True] )
    len(sp)
    #m = folium.Map(location=[42.092422, 11.795413],zoom_start = 5)
    pd_port_ita=get_italian_ports_fitted()
    m=visualize_hexagonsDF(hexagons=pd_port_ita,hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="red")
    # Lon, Lat order.
    lines = [
        {
            "coordinates":[ [y,x] for x,y in zip(sp['latitude'],sp['longitude'])],
            "dates": [str(row) for row in sp['dt_pos_utc'] ],
            "color": "blue"
        },    
    ]
    features = [
        {
            "type": "Feature",
            "geometry": {
            "type": "LineString",
            "coordinates": line["coordinates"],
        },
        "properties": {
            "times": line["dates"],
            "style": {
                "color": line["color"],
                "weight": line["weight"] if "weight" in line else 5,
            },
        },
    }
        for line in lines
    ]
    TimestampedGeoJson(
        {
            "type": "FeatureCollection",
            "features": features,
        },
        period="PT1M",
        add_last_point=True,
    ).add_to(m)
    display(m)
    
def get_italian_ports_fitted(res=8):
    url = 'https://raw.githubusercontent.com/istat-methodology/istat-ais/main/data/Porti_ITA_fitted_RES_'+str(res)+'.csv'
    porti = pd.read_csv(url, delimiter=';',encoding= 'ISO-8859-1')
    return(porti)

def displayITAports(res=8):
    m=visualize_hexagonsDF(hexagons=get_italian_ports_fitted(res),hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="green")
    display(m)

def displayNO_ITAports(res=8):
    m=visualize_hexagonsDF(hexagons=get_NO_ITA_ports(res),hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="green")
    display(m) 
   
def get_NO_ITA_ports(res=8):
    url = 'https://raw.githubusercontent.com/istat-methodology/istat-ais/main/data/Porti_WORLD_NO_ITA_K3_RES'+str(res)+'_NO_DUP.csv'
    porti = pd.read_csv(url, delimiter=';',encoding= 'ISO-8859-1')
    return(porti)

def displayRoute(pd_df,    start_date_filter: datetime=None, end_date_filter: datetime = None,res=8):
    if start_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])>=start_date_filter]
    if end_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])<end_date_filter]
     
    pdmmsi_set=pd_df.sort_values(['imo','dt_pos_utc'], ascending=[True, True] )
    len(pdmmsi_set)
    
    pd_port_ita=get_italian_ports_fitted()
    
    m=visualize_hexagonsDF(hexagons=pd_port_ita,hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="red")

    loc_red=[]
    loc_green=[]
    for index,coord in pdmmsi_set.iterrows():
        if coord['sog']>0.1:
            loc_green.append(( coord['latitude'], coord['longitude']))
        else:
            loc_red.append(( coord['latitude'], coord['longitude']))


    for coord in loc_red:
            folium.Marker( location=[ coord[0], coord[1] ], fill_color='#43d9de', radius=8 ).add_to( m )
    folium.PolyLine(loc_green,
                color='green',
                weight=5,
                opacity=0.8).add_to(m)
    display(m) 
   
def visualize_polygon(polyline, color):
    polyline.append(polyline[0])
    lat = [p[0] for p in polyline]
    lng = [p[1] for p in polyline]
    m = folium.Map(location=[sum(lat)/len(lat), sum(lng)/len(lng)], zoom_start=13, tiles='cartodbpositron')
    my_PolyLine=folium.PolyLine(locations=polyline,weight=8,color=color)
    m.add_child(my_PolyLine)
    return m


def initializeEnvs(sparkLocal):
    sparkLocal.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    sparkLocal.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
    sparkLocal.conf.set("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    sparkLocal.conf.set("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    sparkLocal.conf.set("spark.kryoserializer.buffer.mb","128")
    return sparkLocal

from math import radians, sin, cos, sqrt, atan2


###  Function for anomaly detection ###

def compute_haversine_distance(lat1, long1, lat2, long2):
    R = 6371.0  # radius of the Earth in km

    lat1_rad = radians(lat1)
    long1_rad = radians(long1)
    lat2_rad = radians(lat2)
    long2_rad = radians(long2)

    dlon = long2_rad - long1_rad
    dlat = lat2_rad - lat1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


##############################################
############ Function to move columns of a 
############ dataframe
##############################################

def move_column(df, col_name, new_col_position):
    col_position = df.columns.get_loc(col_name)
    
    # if col_name is already in the new_col_position we want it to be, leave it as it is
    if col_position != new_col_position:
        target_col = df.pop(col_name)
        df.insert(new_col_position, target_col.name, target_col)
        
        
##############################################
############### Function to add time-space and
############### speed
##############################################

# Add time passed between position (DT_sec) , also in minutes (DT_min)
# Add Distance travelled by ship (Dist_km)
# Add Average Speed of the ship during DT_sec, (Spd_kmh)

# start making a copy of the df, to avoid changing the starting df !!

MAX_TIME_MIN = 60 

def add_time_distance(df):
    
    if 'Dist_km' not in df.columns:
    
        # Only useful columns
         #new_col= ['longitude', 'latitude','dt_pos_utc', 'sog', 'cog', 'rot', 'heading',
              #'H3_int_index_5','H3_int_index_6', 'H3_int_index_7','H3_int_index_8',
              #'imo','vessel_name', 'destination', 'dt_insert_utc','dt_static_utc']
        
        new_col = ['mmsi',"imo","latitude","longitude","vessel_name","shipType",'dt_pos_utc','sog','cog','H3_int_index_8']
        #new_col= ['longitude', 'latitude','dt_pos_utc','sog','H3_int_index_8','imo','vessel_name']

        df = df[ new_col ].copy()

        #Make sure the 'dt_pos_utc' is in TIME format
        df['dt_pos_utc'] = pd.to_datetime( df['dt_pos_utc'] )
        
        # Sort time movement in correct time order, keep the old index 
        df = df.sort_values('dt_pos_utc').reset_index().rename(columns={'index':'old_ind'})
    
    #Compute time passed at each position
    Diff= df['dt_pos_utc'].diff().shift(-1)

    Diff_sec= Diff.dt.total_seconds().round(0)
    Diff_min= (Diff.dt.total_seconds()/60).round(3)

    df_ship = df.copy()
    
    df_ship['DT_sec'] = Diff_sec
    df_ship['DT_min'] = Diff_min

    # Place those columns after 'dt_pos_utc'
    time_col_position= df_ship.columns.get_loc('dt_pos_utc')
    move_column( df_ship, 'DT_sec', time_col_position + 1  )
    move_column( df_ship, 'DT_min', time_col_position + 2  )
    
    # Compute distance , Place subsequent long and lat in the same row, to apply function
    df_ship['Long_2'] = df_ship['longitude'].shift(-1)
    df_ship['Lat_2'] = df_ship['latitude'].shift(-1)
    
    # Function necessary to be applied on DF
   # def compute_distance(row):
    #    coord1 = (row['latitude'], row['longitude'])
    #    coord2 = (row['Lat_2'], row['Long_2'])
    #    return haversine(coord1, coord2, unit='km')
    
    def compute_distance(row):        
        return compute_haversine_distance(row['latitude'], row['longitude'], row['Lat_2'], row['Long_2'])
        
    df_ship['Dist_km'] = df_ship.apply( compute_distance, axis=1 ).round(4)
    move_column( df_ship, 'Dist_km', df_ship.columns.get_loc('DT_min') +1  )
    
    df_ship['Spd_kmh'] = 3600*(df_ship['Dist_km'] / df_ship['DT_sec']).round(4)
    move_column( df_ship, 'Spd_kmh', df_ship.columns.get_loc('Dist_km') +1  )

    df_ship.drop(['Long_2', 'Lat_2'], axis=1, inplace=True)
    
    #display(df_ship.head(5))
    return df_ship
