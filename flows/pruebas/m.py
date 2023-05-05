from prefect import flow

from time import time
import pandas as pd
import zipfile
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
import wget
import requests
import logging
import datetime
import os
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

@flow
def my_favorite_function():
    print("What is your favorite number?")
    return 42

@task(log_prints=True)
def load_gcs_bucket():

    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-de-project-bucket-new")

    gcp_cloud_storage_bucket_block.upload_from_path(from_path='/home/ivang/projects/zoomcamp_divvy_project/202301-divvy-tripdata.csv', to_path='raw/202301-divvy-tripdata.csv', timeout=1000)

    print("Loaded to GCS")

@flow(timeout_seconds=10000)
def etl_parent_flow():
    load_gcs_bucket()


if __name__ == "__main__":


    etl_parent_flow()
    # prefect deployment build ./etl_web_to_gcs.py:main_flow -n "First ETL"
    # prefect deployment apply main_flow-deployment.yaml 
    # como forzar tipos en pandas
