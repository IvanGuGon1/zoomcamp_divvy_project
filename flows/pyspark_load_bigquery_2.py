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
min_stream_count = 4
max_stream_count = 6
df_trips = spark.read.format('bigquery').option('table','zoomcamp-de-project-385509.processed.divvy_dataset_2').option('parallelism', max_stream_count).option('minParallelism', min_stream_count).load()

df_trips.registerTempTable('trips')

result = spark.sql("""

SELECT

COUNT(*)

FROM 
    trips

LIMIT 100

""")




result.show(5)




df_trips.write \
  .format("bigquery") \
  .option("temporaryGcsBucket","dataproc-staging-europe-west6-925353208794-ci2rxhel") \
  .mode('overwrite') \
  .save("zoomcamp-de-project-385509.develop.prueba_direct")

