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
def load_gcs_bucket(df:pd.DataFrame,source_path:str,target_path:str):

    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-de-project-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=source_path, to_path=target_path, timeout=1000)
    print("Loaded to GCS")

@flow(timeout_seconds=10000)
def etl_parent_flow():
    url = 'https://divvy-tripdata.s3.amazonaws.com/' + '202301-divvy-tripdata' + '.zip'
    wget.download(url)
    with zipfile.ZipFile('202301-divvy-tripdata' + '.zip', 'r') as zip_ref:
        nombres_archivos = zip_ref.extractall('data/raw')
    df = pd.read_csv('data/raw/202301-divvy-tripdata.csv')
    print(df.head())
    load_gcs_bucket(df,'data/raw/202301-divvy-tripdata.csv',"gs://divvy_bike_sharing/202301-divvy-tripdata.csv")


if __name__ == "__main__":

    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    year = '2021'

    etl_parent_flow()
    # prefect deployment build ./etl_web_to_gcs.py:main_flow -n "First ETL"
    # prefect deployment apply main_flow-deployment.yaml 
    # como forzar tipos en pandas
