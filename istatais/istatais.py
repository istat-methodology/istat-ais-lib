import pandas as pd
from datetime import datetime
import numpy as np

from math import radians, sin, cos, sqrt, atan2

import h3
import h3.api.numpy_int as h3int

from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.types import NullType
from pyspark.sql.functions import pandas_udf, PandasUDFType, col,to_timestamp,desc
import pyspark.sql.functions as F
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
from io import StringIO
from urllib import parse
import folium
from folium import plugins
from folium.plugins import TimestampedGeoJson
import requests
import matplotlib.colors as mcolors
import random

def get_color(cog):
    
    if (cog >= 0.0) & (cog <= 90.0):
        return "purple";
    elif (cog > 90.0) & (cog <= 180.0):
        return "green";
    elif (cog > 180.0) & (cog <= 270.0):
        return "blue";
    else:
        return "yellow";

def random_color_generator():
    color = random.choice(list(mcolors.CSS4_COLORS.keys()))
    return color

def visualize_hexagons(hexagons, color="red", map_center=None, folium_map=None):
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

    if map_center is None:
        map_center = [sum(lat)/len(lat),sum(lng)/len(lng)]
    
    if folium_map is None:
        m = folium.Map(location=[map_center[0], map_center[1]], zoom_start=6, tiles='cartodbpositron')
    else:
        m = folium_map
    for polyline in polylines:
        my_PolyLine=folium.PolyLine(locations=polyline,weight=8,color=color)
        m.add_child(my_PolyLine)
    return m

def visualize_hexagonsDF(hexagons,hexagons_field, hexagons_label,color="red", map_center=None, folium_map=None):
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

    if map_center is None:
        map_center = [sum(lat)/len(lat),sum(lng)/len(lng)]
    
    if folium_map is None:
        m = folium.Map(location=[map_center[0], map_center[1]], zoom_start=6, tiles='cartodbpositron')
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
    
    center_lat_long = [pd_df["latitude"].mean(),pd_df["longitude"].mean()]
    m = folium.Map(location=[center_lat_long[0],center_lat_long[1]])
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
    #m = folium.Map(location=[42.092422, 11.795413],zoom_start = 6)
    pd_port_ita=get_italian_ports_fitted()
    
    center_lat_long = [pd_df["latitude"].mean(),pd_df["longitude"].mean()]
    m=visualize_hexagonsDF(hexagons=pd_port_ita,hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="red",map_center = center_lat_long)
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
    url = 'https://raw.githubusercontent.com/istat-methodology/istat-ais-lib/main/data/Porti_ITA_fitted_RES_'+str(res)+'.csv'
    porti = pd.read_csv(url, delimiter=';',encoding= 'ISO-8859-1')
    return(porti)

def displayITAports(res=8):
    m=visualize_hexagonsDF(hexagons=get_italian_ports_fitted(res),hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="green")
    display(m)

def displayNO_ITAports(res=8):
    m=visualize_hexagonsDF(hexagons=get_NO_ITA_ports(res),hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="green")
    display(m) 
   
def get_NO_ITA_ports(res=8):
    url = 'https://raw.githubusercontent.com/istat-methodology/istat-ais-lib/main/data/Porti_WORLD_NO_ITA_K3_RES'+str(res)+'_NO_DUP.csv'
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
    
    center_lat_long = [pd_df["latitude"].mean(),pd_df["longitude"].mean()]    
    m=visualize_hexagonsDF(hexagons=pd_port_ita,hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="red",map_center = center_lat_long)

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


def displayRoute_points(pd_df, start_date_filter: datetime=None, end_date_filter: datetime = None,res=8):
    if start_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])>=start_date_filter]
    if end_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])<end_date_filter]
     
    pdmmsi_set=pd_df.sort_values(['imo','dt_pos_utc'], ascending=[True, True] )
    #len(pdmmsi_set)
    
    pd_port_ita=get_italian_ports_fitted()
        
    center_lat_long = [pd_df["latitude"].mean(),pd_df["longitude"].mean()]
    m=visualize_hexagonsDF(hexagons=pd_port_ita,hexagons_field='H3_hex_'+str(res), hexagons_label='UNLocode',color="red", map_center=center_lat_long)

    loc_red=[]
    loc_green=[]

    for index,coord in pdmmsi_set.iterrows():
        
        
        if coord['sog']>0.1:
            loc_green.append(( coord['latitude'], coord['longitude'], coord['sog'], coord['cog'], coord['dt_pos_utc'], coord['heading']))
        else:
            loc_red.append(( coord['latitude'], coord['longitude'], coord['sog'], coord['cog'], coord['dt_pos_utc'], coord['heading']))

    for coord in loc_green:
            label = "SOG: "+ str(coord[2]) + "; COG: " + str(coord[5]) + "\n\n" + str(coord[4])
            rotation =coord[5]
            folium.RegularPolygonMarker(location=[ coord[0], coord[1] ], color =get_color(coord[3]), fill='True', number_of_sides=3, radius=10, rotation=rotation, popup=label).add_to(m)
    
    for coord in loc_red:
            folium.Marker( location=[ coord[0], coord[1] ], fill_color='#43d9de', color ='red', radius=8).add_to( m )           

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
        
        #new_col = ['mmsi',"imo","latitude","longitude","vessel_name",'dt_pos_utc','sog','cog','destination','eta','H3_int_index_8']
        #new_col= ['longitude', 'latitude','dt_pos_utc','sog','H3_int_index_8','imo','vessel_name']
        new_col = ['mmsi','imo','callsign','latitude','longitude','vessel_name','dt_pos_utc','sog','cog','nav_status',
            'flag_code','destination','H3_int_index_8','gt','shipType','NT','DWT','cod_Eurostat']

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


##############################################
############### Funzione Conta Sparizioni 
##############################################

# The function takes a dataframe of a ship route and a Time delay in minutes
# it checks if the ship disappeared for more than max_time_min minutes and also it reappered
# having travelled for too short of a lenght ( seen as average_speed < min_speed)

# if it doesn't, it gives an EMPTY LIST
# if it does, it gives the number of such disappearances
# and it gives the list with the indexes of the dataframe when the vanishing occurred

# it also displays a dataframe with information of the disappearances

# Also, the function stops if the max_time_delay is too short (20 minutes)
MAX_TIME_MIN=60
# @pandas_udf("mmsi int, imo int, shipType string, n_disappear int", PandasUDFType.GROUPED_MAP)
def time_jumps_min_old(df):

    print('**time_jumps_min')
    df_imo = df['imo'].unique()
    imo = df_imo[0]
    
    df_mmsi = df['mmsi'].unique()
    mmsi = df_mmsi[0]
    
    df_shipType = df['shipType'].unique()
    shipType = df_shipType[0]
    
    # Too short a time causes huge output, 
    # the function is meant to find big delayas
    if MAX_TIME_MIN <= 20:
        print('Chose a Bigger time')
        return
    
    # Add Time passed between each position (DT_sec) 
    # Add Distance travelled (Dist_km)    
    
    ship_td = add_time_distance(df)
        
    min_speed= 15
    # natural way to get a minimum speed:
    #mask_vd0=  df['Spd_kmh'] >0
    #min_speed_2= df.loc[mask_vd0]['Spd_kmh'].quantile(0.20).round(2)
    # print('velocità minima, 20-o percentile: ', min_speed_2)
    
    # Check if time delay is greater than max_time, Saves indeces of those occurences
    
    time_delay_indeces= []
    result_df = pd.DataFrame({'mmsi': pd.Series(dtype='int'),
                   'imo': pd.Series(dtype='int'),
                   'shipType': pd.Series(dtype='str'),
                   'n_disappear': pd.Series(dtype='int')})

    n_disappear= int(len(time_delay_indeces)/2)
    
    if ( ship_td['DT_min'] > MAX_TIME_MIN).any():
        #print(f' "Delays, Long_Time + Slow_speed" :  \n')
        
        mask_disappear_slow=(ship_td['DT_min']> MAX_TIME_MIN)&(ship_td['Spd_kmh']< min_speed)&(ship_td['Spd_kmh']>0.1)
        mask_disappear_slow = mask_disappear_slow | mask_disappear_slow.shift(+1)
        
        ship_td_disappear_and_slow = ship_td.loc[mask_disappear_slow]
        #display(ship_td_disappear_and_slow)
        
        delays_indeces_new= ship_td_disappear_and_slow.index
        delays_indeces_old= ship_td.iloc[delays_indeces_new]['old_ind'].values

        # Also add indexes when ship reappeared, they are just the subsequent indexes
        # Print a dataframe with useful data at time delays, and delays in minutes
        
        time_delay_indeces= delays_indeces_old.tolist()
        n_disappear= int(len(time_delay_indeces)/2)
        ##return n_disappear, time_delay_indeces
        
        new_row = {'mmsi': mmsi,'imo':imo,'shipType':shipType,'n_disappear':n_disappear}
        result_df = result_df.append(new_row, ignore_index=True)        
        return(result_df)
    else:
        print('**Good, No Delays')
    
    new_row = {'mmsi': mmsi,'imo':imo,'shipType':shipType,'n_disappear':n_disappear}
    result_df = result_df.append(new_row, ignore_index=True)
    return(result_df)
    ##return n_disappear, time_delay_indeces
