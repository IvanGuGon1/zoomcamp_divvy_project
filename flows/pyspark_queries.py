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
    .appName('Python to BIGQUERY queries') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-staging-europe-west6-925353208794-ci2rxhel')
min_stream_count = 4
max_stream_count = 6
df_trips = spark.read.format('bigquery').option('table','zoomcamp-de-project-385509.processed.divvy_dataset_2023').option('parallelism', max_stream_count).option('minParallelism', min_stream_count).load()

df_trips.registerTempTable('trips')

result_desc = spark.sql("""

SELECT
 DATE_FORMAT(from_utc_timestamp(date, 'UTC'), 'yyyy-MM-dd') AS day,
  AVG(distance) AS distance,
  icon,
  tempmax
FROM
  trips
GROUP BY
  day,icon,tempmax
ORDER BY
  distance DESC;

""")

result_desc.show(10)                        

result_asc = spark.sql("""

SELECT
 DATE_FORMAT(from_utc_timestamp(date, 'UTC'), 'yyyy-MM-dd') AS day,
  AVG(distance) AS distance,
  icon,
  tempmax
FROM
  trips
GROUP BY
  day,icon,tempmax
ORDER BY
  distance ASC;

""")


result_asc.show(10)


'''

df_trips.write \
  .format("bigquery") \
  .option("temporaryGcsBucket","dataproc-staging-europe-west6-925353208794-ci2rxhel") \
  .mode('overwrite') \
  .save("zoomcamp-de-project-385509.develop.prueba_direct")
'''
