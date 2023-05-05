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

spark = SparkSession.builder \
    .appName('Python to BIGQUERY TEST') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-staging-europe-west6-925353208794-ci2rxhel')

df_trips = spark.read.parquet(data_input,inferSchema=True)

df_trips_with_date = df_trips.withColumn("date", to_date("started_at", "yyyy"))
df_trips_with_date_casted = df_trips_with_date.withColumn("date", col("date").cast(StringType()))

print("columns en df_trips")
print(df_trips_with_date_casted.columns)

df_weather = spark.read.parquet('gs://divvy_bike_sharing/raw/chicago_historical_weather.parquet',inferSchema=True)
df_weather_renamed = df_weather \
    .withColumnRenamed('datetime', 'date') 
df_weather_renamed_casted = df_weather_renamed.withColumn("date", col("date").cast(StringType()))
print("columns en weather")
print(df_weather_renamed_casted.columns)

joined_df = df_trips_with_date_casted.join(df_weather_renamed_casted, on="date", how="inner")
print("columnas n joined_df")
print(joined_df.columns)

'''
joined_df.write.mode("overwrite").format('bigquery') \
    .option('table', output) \
    .save()
'''
#joined_df.write.format("bigquery").option("writeMethod", "direct").save("develop.prueba_direct")

joined_df.write \
  .format("bigquery") \
  .option("temporaryGcsBucket","dataproc-staging-europe-west6-925353208794-ci2rxhel") \
  .save("develop.prueba_direct")