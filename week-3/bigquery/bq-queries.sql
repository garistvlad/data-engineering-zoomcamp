/*
Home Assignment #3. Vladislav Garist. BigQuery
*/


-- Create external table from FHV data for 2019:
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_fhv_taxi_trips_2019`
OPTIONS (
  format="CSV",
  uris=[
    'gs://de_zoomcamp_data_lake_hip-plexus-374912/taxi/fhv_tripdata_2019-*.csv.gz'
  ]
);


-- TEST: All months have been loaded:
select date_trunc(pickup_datetime, MONTH) as d_month
from `nytaxi.external_fhv_taxi_trips_2019`
group by 1
order by 1
;


-- Create this table in BQ
CREATE OR REPLACE TABLE `nytaxi.fhv_taxi_trips_2019`
AS (
  select *
  from `nytaxi.external_fhv_taxi_trips_2019`
)
;

/*
Question 1: What is the count for fhv vehicle records for year 2019?

Result: 43,244,696
*/
select count(1)
from `nytaxi.fhv_taxi_trips_2019`
;



/*
Question 2: Query to get distinct number of `affiliated_base_number`

Estimation:
- External table: "This query will process 0 B when run."
- BigQuery table: "This query will process 317.94 MB when run."

Result: 3,165
*/

-- BQ
select count(distinct Affiliated_base_number)
from `nytaxi.fhv_taxi_trips_2019`
;

-- External
select count(distinct Affiliated_base_number)
from `nytaxi.external_fhv_taxi_trips_2019`
;


/*
Querstion 3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

Result: 717,748
*/

select count(1)
from `nytaxi.fhv_taxi_trips_2019`
where true
  and PUlocationID is null
  and DOlocationID is null
;



/*
Question 4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Answer:
- Use pickup_datetime for partitioning due to further filtering
- Use affiliated_base_number for clustering due to need top be ordered by
*/

-- Create partitioned & clustered table
CREATE OR REPLACE TABLE `nytaxi.fhv_taxi_trips_2019_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS (
  select *
  from `nytaxi.fhv_taxi_trips_2019`
)
;

-- TEST: partitioned was applied (query processing decreased)
select *
from `nytaxi.fhv_taxi_trips_2019_partitioned`
where date(pickup_datetime) between '2019-01-01' and '2019-03-01'
;



/*
Question 5: Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).

Estimation:
- Before partitioning: This query will process 647.87 MB when run.
- After partitioning: This query will process 23.05 MB when run.
*/

-- Before partitioning:
select count(distinct Affiliated_base_number)
from `nytaxi.fhv_taxi_trips_2019`
where true
  and date(pickup_datetime) between '2019-03-01' and '2019-03-31'
;

-- After partitioning:
select count(distinct Affiliated_base_number)
from `nytaxi.fhv_taxi_trips_2019_partitioned`
where true
  and date(pickup_datetime) between '2019-03-01' and '2019-03-31'
;



/*
Question 6: Where is the data stored in the External Table you created?

Answer: In GCS (GCP Bucket) where it was initially loaded to
*/


/*
Question 7: It is best practice in Big Query to always cluster your data:

Answer: No, in case of extensive Write/Updates it might be inefficient to recluster data every time
*/


/*
Question 8:
A better format to store these files may be parquet.
Create a data pipeline to download the gzip files and convert them into parquet.
Upload the files to your GCP Bucket and create an External and BQ Table.
Note: Column types for all files used in an External Table must have the same datatype.
While an External Table may be created and shown in the side panel in Big Query,
this will need to be validated by running a count query on the External Table to check if any errors occur.
*/

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_fhv_taxi_trips_2019`
OPTIONS (
  format="PARQUET",
  uris=[
    'gs://de_zoomcamp_data_lake_hip-plexus-374912/taxi/fhv_tripdata_*.parquet'
  ]
);

-- TEST: format is OK, executes without errors
select count(*)
from `nytaxi.external_fhv_taxi_trips_2019`
;
