

"""
Transformaciones que necesito.

1 - Drop NA
2 - ransformation_add_distance_column(df)
3 - transformation_add_travel_time_column(df)
4 - transformation_add_cost_columns(df)
"""

def calculate_distance(row):
    start_coords = (row['start_lat'], row['start_lng']) # coordenadas de origen
    end_coords = (row['end_lat'], row['end_lng']) # coordenadas de destino
    distance_metres = distance.distance(start_coords, end_coords).m # calcular la distancia en metros
    return round(distance_metres,2)

def calculate_cost(row):
    bike_type = row['rideable_type']
    member_casual = row['member_casual']
    distance = row['distance']
    total_price = 0
    if bike_type == 'electric_bike':
        if member_casual == 'member':
            total_price = total_price + ( 1 + (0.17 * float(distance)))
        elif member_casual == 'casual':
            total_price = total_price + (1 + (0.42 * float(distance)))
    elif bike_type == 'classic' or bike_type == 'docked_bike':
        if member_casual == 'member':
            total_price = total_price + ( 1 + (0.07 * float(distance)))
        elif member_casual == 'casual':
            total_price = total_price +  (1 + (0.15 * float(distance)))
            
    return total_price

#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types


parser = argparse.ArgumentParser()

parser.add_argument('--data_input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

data_input = args.data_input
output = args.output


spark = SparkSession.builder \
    .appName('Python to BIGQUERY') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-staging-europe-west6-925353208794-ci2rxhel')

df = spark.read.parquet(data_input,inferSchema=True)
print("-------")
print(data_input)
print(df.printSchema())
print("-------")

df.registerTempTable('bike_trips')

df_result = spark.sql("""

SELECT

    ride_id,
    rideable_type,
    EXTRACT(YEAR FROM started_at) AS year_column,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual

FROM 
    bike_trips

LIMIT 100

""")

df_result.show(5)