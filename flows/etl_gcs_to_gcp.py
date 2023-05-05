from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from geopy import distance


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


def calculate_distance(row):
    start_coords = (row['start_lat'], row['start_lng']) # coordenadas de origen
    end_coords = (row['end_lat'], row['end_lng']) # coordenadas de destino
    distance_metres = distance.distance(start_coords, end_coords).m # calcular la distancia en metros
    return round(distance_metres,2)


@task(retries=3)
def extract_from_gcs(year, month) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"raw/{year}{month}-divvy-tripdata.parquet"
    gcs_block = GcsBucket.load("zoomcamp-de-project-bucket-new")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"/home/ivang/projects/zoomcamp_divvy_project/data/temp/")
    return Path(f"/home/ivang/projects/zoomcamp_divvy_project/data/temp/raw/{year}{month}-divvy-tripdata.parquet")

@task(log_prints=True)
def transform_drop_na(path:Path):
    df = pd.read_parquet(path)
    print(df.head())
    print(len(df))
    df = df.dropna()
    return df

@task(log_prints=True)
def transform_types(df:pd.DataFrame):
    #df.started_at = pd.to_datetime(df.started_at)
    df.loc[:, 'started_at'] = pd.to_datetime(df['started_at'])
    df.loc[:, 'ended_at'] = pd.to_datetime(df['ended_at'])
 
    print("Transformed types")
    return df



@task(log_prints=True)
def transformation_add_distance_column(df:pd.DataFrame):
    #distance in meters
    df.loc[:, 'distance'] = df.apply(calculate_distance, axis=1)
    return df


@task(log_prints=True)
def transformation_add_travel_time_column(df:pd.DataFrame):
    #time in minutes
    df['travel_time'] = (df['ended_at'] - df['started_at']) / pd.Timedelta(minutes=1)
    df['travel_time'] = round(df['travel_time'], 1)
    return df

@task(log_prints=True)
def transformation_add_cost_columns(df:pd.DataFrame):
    df["cost"] = df.apply(calculate_cost, axis=1)
    df['cost'] = round(df['cost'], 3)
    return df


@task(log_prints=True)
def transform_join_dataframes(df:pd.DataFrame):

    df['date'] = df['started_at'].dt.date
    df['date'] = df['date'].astype(str)

    weather_df = pd.read_parquet('chicago_historical_weather.parquet')
    weather_df = weather_df.rename(columns={'datetime': 'date'})
    weather_df['date'] = weather_df['date'].astype(str)
    combined_df = df.merge(weather_df, on='date')
    print(combined_df.head())
    return combined_df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-new-credentials")

    df.to_gbq(
        destination_table="processed.divvy_dataset_2022_2023",
        project_id="zoomcamp-de-project-385509",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year,month):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs( year, month)

    df = transform_drop_na(path)
    df = transform_types(df)
    df = transformation_add_distance_column(df)
    df = transformation_add_travel_time_column(df)
    df = transformation_add_cost_columns(df)
    df = transform_join_dataframes(df)

    write_bq(df)

@flow()
def etl_parent_flow(
    months: list[str], year: int
):
    for month in months:
        etl_gcs_to_bq(year, month)


if __name__ == "__main__":

    months = ['01','02','03','04', '05', '06', '07', '08', '09', '10', '11','12']

    year = 2022
    etl_parent_flow(months, year)