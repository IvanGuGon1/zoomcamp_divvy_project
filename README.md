
**DIVVY BIKES SHARING, CHICAGO**

The selected is dataset of divvy bikes, a bicycle rental company that operates in the city of Chicago.
The objective is to enrich the divvy bikes dataset, which contains information related to the dates of each use, coordinates and type of service, with information related to Chicago's meteorological history, to check how weather affects the number of average daily kilometers route.


Information added to the dataset:

Kilometers traveled - It has been calculated with the geopy library and the coordinates.

Travel time - It is obtained by subtracting the start and end timestamp.

Cost per service - Price information for the use of bicycles has been obtained from the divvy website, multiplied by travel time.

Historical weather information - It has been obtained from the website visualcrossing.com, I am especially interested in the maximum temperature, the wind, and the general weather.


**USED ​​TECHNOLOGY**

Orchestrator - Prefect Cloud.
Cloud Provider - Google Cloud
Datalake - Google Cloud Storage
DataWareHouse - Big Query
DashBoard - Google Data Studio.
Big Data Technology - Pyspark in DataProc.
Programming language - Python


**ARCHITECTURE**.

Pipeline 1 - flows/**etl_web_to_gcs_simple.py** - The files are downloaded from the official website, 1 monthly file, they are decompressed, converted to parquet and uploaded to the Datalake in Google Cloud Storage
Pipeline 2 - flows/**etl_gcs_to_gcp.py** - Datalake parquet files are obtained, data cleaning is performed, and various transformations and join with historical weather information are performed.
PySpark queries - flows/**pyspark_queries.py**. Queries are made to the DataWarehouse through PySpark on DataProc to know the aggregated data for the years 2022 and 2023. The info that if offer us is how the weather affect on the daily use of bicicles, using general weather info and maximum temperatura.
Dashboard - A dashboard with the information required in DataStudio. Link:
https://lookerstudio.google.com/s/nW-6tIi1FWM
