
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

from prefect import flow, task, get_run_logger



@task(log_prints=True)
def check_url(url:str) -> bool:

    print(f"Checking url {url}")
    try:
        respuesta = requests.get(url)
        if respuesta.status_code == 200:
            print(f"El archivo {url} está disponible.")
            return True
        else:
            print(f"El archivo {url} NO está disponible.")
            return False
        
    except requests.exceptions.RequestException as e:
        print(f"El archivo no está disponible. Error: {e}")
        return False


@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str,filename: str,folder: str):

    print(f"Starting download url {url}")

    data_folder = '../data/' + folder
    if os.path.isfile(filename + '.zip'):
        os.remove(filename + '.zip')
        wget.download(url,out=data_folder+filename + '.zip')
    else:
        wget.download(url,out=data_folder+filename + '.zip')

    with zipfile.ZipFile(data_folder + filename + '.zip', 'r') as zip_ref:
        nombres_archivos = zip_ref.namelist()

        for nombre_archivo in nombres_archivos:
            print(f"Nombre del fichero dentro del zip {nombre_archivo}")
            print(f"Nombre que quiero darle {filename + '.csv'}")
            zip_ref.extract(nombre_archivo)
            nombre_anterior = nombre_archivo
            nombre_nuevo = filename + '.csv' 
            os.rename(nombre_anterior, nombre_nuevo)
            df = pd.read_csv(nombre_nuevo)
            print(df.head())
            return df
        
        return None


@task(log_prints=True)
def transform_types(df:pd.DataFrame):
    #df.started_at = pd.to_datetime(df.started_at)
    df.loc[:, 'started_at'] = pd.to_datetime(df['started_at'])
    df.loc[:, 'ended_at'] = pd.to_datetime(df['ended_at'])
 
    print("Transformed types")
    return df



@task(log_prints=True)
def transform_to_parquet(df:pd.DataFrame,filename,folder):
    #TODO: Add more cleaning steps

    data_folder = '../data/' + folder
        
    df.ride_id = df.ride_id.astype(str)
    df.rideable_type = df.rideable_type.astype(str)
    df.started_at = df.started_at.astype('datetime64[ns]')
    df.ended_at = df.ended_at.astype('datetime64[ns]')
    df.start_station_name = df.start_station_name.astype(str)
    df.start_station_id = df.start_station_id.astype(str)
    df.end_station_name = df.end_station_name.astype(str)
    df.end_station_id = df.end_station_id.astype(str)
    df.start_lat = df.start_lat.astype(float)
    df.start_lng = df.start_lng.astype(float)
    df.end_lat = df.end_lat.astype(float)
    df.end_lng = df.end_lng.astype(float)
    df.member_casual = df.member_casual.astype(str)




    print("Dataframe cleaned Dataframe types: ", df.dtypes)
    df.to_parquet(data_folder + filename + '.parquet',index=False)
    print(df.head())
    return data_folder + filename + '.parquet'


@task(log_prints=True)
def load_gcs_bucket(df:pd.DataFrame,source_path:str,target_path:str):

    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-de-project-bucket-new")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=source_path, to_path=target_path, timeout=1000)
    print("Loaded to GCS")

@flow(name="Ingest Data")
def main_flow(year:str,month:str,folder:str):

    filename = f'{year}{month}-divvy-tripdata'
    url = 'https://divvy-tripdata.s3.amazonaws.com/' + filename + '.zip'
    source_path = '/home/ivang/projects/zoomcamp_divvy_project/flows/' + filename + '.parquet'

    target_path = folder + filename + '.parquet'
    valid_url = check_url(url)
    if valid_url == True:
        df = extract_data(url,filename,folder)
 
        df = transform_types(df)

        source_path = transform_to_parquet(df,filename,folder)
        load_gcs_bucket(df,source_path,target_path)
        print("Finished")
        print("---------------------------------")
    else:
        print("Carga de datos no realizada")

  

@flow(timeout_seconds=10000)
def etl_parent_flow(months:list[str], year:str,folder:str):

    for month in months:
        main_flow(year, month, folder)
        print(f"Loaded month {month} , {year} ")

if __name__ == "__main__":

    months = ['01','02','03','04','05','06','07','08','09','10','11','12']
    # bad filez : 4, 
    months = ['12']
    year = '2020'
    #raw/ processed/ develop/
    #test case of url url = 'https://divvy-tripdata.s3.amazonaws.com/202301-divvy-tripdata.zip
    #url_for_test = 'https://divvy-tripdata.s3.amazonaws.com/202301-divvy-tripdata.zip'
    folder = 'raw/'
    etl_parent_flow(months,year,folder)
    # prefect deployment build ./etl_web_to_gcs.py:main_flow -n "First ETL"
    # prefect deployment apply main_flow-deployment.yaml 
    # como forzar tipos en pandas
