Run the python scripts as guided in the 
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/homework.md

<!-- Load the files to Big query -->

CREATE OR REPLACE EXTERNAL TABLE `de-camp-expert.ny_taxi.yellow_taxi_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
  'gs://decamp_buck/yellow_tripdata_2024-01.parquet',
  'gs://decamp_buck/yellow_tripdata_2024-02.parquet',
  'gs://decamp_buck/yellow_tripdata_2024-03.parquet',
  'gs://decamp_buck/yellow_tripdata_2024-04.parquet',
  'gs://decamp_buck/yellow_tripdata_2024-05.parquet',
  'gs://decamp_buck/yellow_tripdata_2024-06.parquet'
]
);



<!-- /* After laoding the parquet files */ -->

CREATE OR REPLACE TABLE `de-camp-expert.ny_taxi.yellow_tripdata`
AS
SELECT *
FROM `de-camp-expert.ny_taxi.yellow_taxi_tripdata_external`;

<!-- Q! -->
SELECT COUNT(*) AS total_rows
FROM `de-camp-expert.ny_taxi.yellow_tripdata`;

<!-- Q2 -->
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocations
FROM `de-camp-expert.ny_taxi.yellow_taxi_tripdata_external`;

Highlight the select query to estiamte the byte size

SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocations
FROM `de-camp-expert.ny_taxi.yellow_tripdata`;

<!-- Q4 -->
SELECT COUNT(*) AS zero_fare_count
FROM `de-camp-expert.ny_taxi.yellow_tripdata`
WHERE fare_amount = 0;

<!-- Q5 -->
CREATE OR REPLACE TABLE `de-camp-expert.ny_taxi.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)  -- partitioning by dropoff date
CLUSTER BY VendorID                       -- clustering by VendorID
AS
SELECT *
FROM `de-camp-expert.ny_taxi.yellow_tripdata`;  -- materialized table


