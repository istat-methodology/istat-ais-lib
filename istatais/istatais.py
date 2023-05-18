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
        m = folium.Map(location=[sum(lat)/len(lat), sum(lng)/len(lng)], zoom_start=13, tiles='cartodbpositron')
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
        label=row[hexagons_label]+':'+row[hexagons_field]
        labels.append(label)
        polygons = h3.h3_set_to_multi_polygon([hex], geo_json=False)
        # flatten polygons into loops.
        outlines = [loop for polygon in polygons for loop in polygon]
        polyline = [outline + [outline[0]] for outline in outlines][0]
        lat.extend(map(lambda v:v[0],polyline))
        lng.extend(map(lambda v:v[1],polyline))
        polylines.append(polyline)
    
    if folium_map is None:
        m = folium.Map(location=[sum(lat)/len(lat), sum(lng)/len(lng)], zoom_start=13, tiles='cartodbpositron')
    else:
        m = folium_map
    for index,polyline in enumerate(polylines):
        my_PolyLine=folium.PolyLine(locations=polyline,tooltip=labels[index],weight=8,color=color,popup=labels[index])
      
        m.add_child(my_PolyLine)
    return m

def displayRoute(pd_df,    start_date_filter: datetime=None, end_date_filter: datetime = None):
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

def trackRoute(pd_df,    start_date_filter: datetime=None, end_date_filter: datetime = None):
    if start_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])>=start_date_filter]
    if end_date_filter is not None:
        pd_df=pd_df[pd.to_datetime(pd_df['dt_pos_utc'])<end_date_filter]
     
    sp=pd_df.sort_values(['imo','dt_pos_utc'], ascending=[True, True] )
    len(sp)
    m = folium.Map(location=[42.092422, 11.795413],zoom_start = 5)
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
