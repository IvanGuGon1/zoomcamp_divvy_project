

"""
Transformaciones que necesito.

1 - Drop NA
2 - ransformation_add_distance_column(df)
3 - transformation_add_travel_time_column(df)
4 - transformation_add_cost_columns(df)
5 - Join
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
from pyspark.sql.functions import to_date
import argparse
from google.cloud.bigquery import SchemaField

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.types import StringType
from pyspark.sql.functions import col


parser = argparse.ArgumentParser()

parser.add_argument('--data_input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

data_input = args.data_input
output = args.output

#TODO: Seleccionar solo las columnas que necesito, mirar como puedo mejorar el rendimiento, aumentar la capacidad.
spark = SparkSession.builder \
    .appName('Python to BIGQUERY') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-staging-europe-west6-925353208794-ci2rxhel')

df_trips = spark.read.parquet(data_input,inferSchema=True)
df_trips_with_date = df_trips.withColumn("date", to_date("started_at", "yyyy"))
df_trips_with_date_casted = df_trips_with_date.withColumn("date", col("date").cast(StringType()))
df_trips_with_date_casted.show(5)
df_trips_with_date_casted.printSchema()

#df_trips_with_date = df_trips.withColumn("date", to_date("started_at", "yyyy-MM-dd HH:mm:ss"))
#df_trips_with_date_casted = df_trips_with_date.withColumn("date", col("date").cast(StringType()))

df_trips_with_date_casted.registerTempTable('bike_trips')


#   EXTRACT(YEAR FROM started_at) AS year_column,
df_trips_with_date_casted_result = spark.sql("""

SELECT
    date,
    ride_id,
    rideable_type,
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

df_trips_with_date_casted_result.show(5)


#   EXTRACT(YEAR FROM started_at) AS year_column,


print("-------")
df_weather = spark.read.parquet('gs://divvy_bike_sharing/raw/chicago_historical_weather.parquet',inferSchema=True)
print("weather dataframe")

df_weather_renamed = df_weather \
    .withColumnRenamed('datetime', 'date') 
df_weather_renamed_casted = df_weather_renamed.withColumn("date", col("date").cast(StringType()))


df_weather_renamed_casted.registerTempTable('weather')

df_weather_renamed_casted_result = spark.sql("""

SELECT

    *

FROM

    weather

LIMIT 100

""")

df_weather_renamed_casted.show(5)
df_weather_renamed_casted.printSchema()


print("Hacemos el join")
joined_df = df_trips_with_date_casted.join(df_weather_renamed_casted, on="date", how="inner")
joined_df.show(5)
joined_df.printSchema()
#joined_df.drop('__index_level_0__')
'''
spark_schema = df_result.schema
# Convierte el schema de PySpark en un schema de BigQuery.
def convert_spark_type_to_bigquery_type(spark_type):
    type_mapping = {
        'ByteType': 'INT64',
        'ShortType': 'INT64',
        'IntegerType': 'INT64',
        'LongType': 'INT64',
        'FloatType': 'FLOAT64',
        'DoubleType': 'FLOAT64',
        'DecimalType': 'NUMERIC',
        'StringType': 'STRING',
        'BinaryType': 'BYTES',
        'BooleanType': 'BOOL',
        'TimestampType': 'TIMESTAMP',
        'DateType': 'DATE',
    }
    return type_mapping.get(spark_type, 'STRING')

def convert_spark_field_to_bigquery_field(spark_field):
    return SchemaField(spark_field.name, convert_spark_type_to_bigquery_type(spark_field.dataType.typeName()))

bigquery_schema = [convert_spark_field_to_bigquery_field(field) for field in spark_schema.fields]

# Imprime el schema en formato de texto.
for field in bigquery_schema:
    print(f"{field.name}: {field.field_type}")
'''

#TODO: Mirar en chatgpt si hay alguna forma de hacerlo mas eficiente.





joined_df.write.mode("overwrite").format('bigquery') \
    .option('table', output) \
    .option("compression", "snappy") \
    .save()
