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
import base64  

import json
import pandas as pd
from io import StringIO
from urllib import parse

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
