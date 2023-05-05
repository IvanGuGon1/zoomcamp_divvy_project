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

spark = SparkSession.builder \
    .appName('JOIN') \
    .getOrCreate()


print("------")
print("TRIPS")
df_trips = spark.read.parquet('202209-divvy-tripdata.parquet',inferSchema=True)
df_trips_with_date = df_trips.withColumn("date", to_date("started_at", "yyyy"))
df_trips_with_date_casted = df_trips_with_date.withColumn("date", col("date").cast(StringType()))
df_trips_with_date_casted.show(5)
df_trips_with_date_casted.printSchema()


print("------")
print("WEATHER")
df_weather = spark.read.parquet('chicago_historical_weather.parquet',inferSchema=True)
df_weather_renamed = df_weather \
    .withColumnRenamed('datetime', 'date') 
df_weather_renamed_casted = df_weather_renamed.withColumn("date", col("date").cast(StringType()))

df_weather_renamed_casted.show(5)
df_weather_renamed_casted.printSchema()

print("-------")
print("---JOIN----")

joined_df = df_trips_with_date_casted.merge(df_weather_renamed_casted, on="date", how="inner")
joined_df.show(5)
joined_df.printSchema()

#joined_df.write.parquet('joined.parquet', mode='overwrite')

if len(df_trips_with_date_casted.columns) == len(set(df_weather_renamed_casted.columns)):
    print('No hay valores duplicados en la lista.')
else:
    print('Hay valores duplicados en la lista.')
'''
spark = SparkSession.builder \
    .appName('JOIN') \
    .getOrCreate()


print("------")
print("TRIPS")
df_trips = spark.read.parquet('202209-divvy-tripdata.parquet',inferSchema=True)


#df_trips.show(5)

df_trips_with_date = df_trips.withColumn("date", to_date("started_at", "yyyy"))
df_trips_with_date_casted = df_trips_with_date.withColumn("date", col("date").cast(StringType()))
df_trips_with_date_casted.show(5)
df_trips_with_date_casted.printSchema()


print("------")
print("WEATHER")
df_weather = spark.read.parquet('chicago_historical_weather.parquet',inferSchema=True)
df_weather_renamed = df_weather \
    .withColumnRenamed('datetime', 'date') 
df_weather_renamed_casted = df_weather_renamed.withColumn("date", col("date").cast(StringType()))

df_weather_renamed_casted.show(5)
df_weather_renamed_casted.printSchema()

print("-------")
print("---JOIN----")

joined_df = df_trips_with_date_casted.join(df_weather_renamed_casted, on="date", how="inner")
joined_df.show(5)
joined_df.printSchema()

joined_df.write.parquet('joined.parquet', mode='overwrite')
'''
'''
spark = SparkSession.builder \
    .appName('JOIN') \
    .getOrCreate()



df_trips = spark.read.parquet('202209-divvy-tripdata.parquet',inferSchema=True)
df_trips.printSchema()
df_trips_selected = df_trips.select('ride_id','rideable_type','started_at','member_casual')


df_trips_selected_renamed = df_trips_selected.withColumnRenamed('started_at', 'date') 
df_trips_with_selected_renamed_year = df_trips_selected_renamed.withColumn("year", to_date("date", "yyyy"))
df_trips_with_selected_renamed_year_casted = df_trips_with_selected_renamed_year.withColumn("year", col("year").cast(StringType()))
df_trips_with_selected_renamed_year_casted.show()
df_trips_with_selected_renamed_year_casted.printSchema()
'''

print("Read 2020")
print("------")
print("TRIPS")
df_trips_2020 = spark.read.parquet('*-divvy-tripdata.parquet',inferSchema=True)
print(df_trips_2020.show())
print(f"ROWS {df_trips_2020.count()}")