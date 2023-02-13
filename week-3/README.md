## Week 3 Homework

The main artefacts related to this home assignment:
- BigQuery supplementary queries: [bq-queries.sql](./bigquery/bq-queries.sql)
- Prefect data pipeline, which loads `parquet` files to Google Cloud Storage: [etl_web_to_gcs.py](./data-pipeline/flow/etl_web_to_gcs.py)

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

**Answer 0:**

- Create external table from SCV files
```bigquery
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
```

- Create table in BQ based on this data
```bigquery
-- Create this table in BQ
CREATE OR REPLACE TABLE `nytaxi.fhv_taxi_trips_2019`
AS (
  select *
  from `nytaxi.external_fhv_taxi_trips_2019`
)
;
```


## Question 1:
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

**Answer 1:**

- BQ Query:
```bigquery
-- Result: 43,244,696
select count(1)
from `nytaxi.fhv_taxi_trips_2019`
;
```

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table

**Answer 2:**

- BQ queries:
```bigquery
/*
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
```

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- 717,748
- 1,215,687
- 5
- 20,332

**Answer 3:**

- BQ query:
```bigquery
-- Result: 717,748
select count(1)
from `nytaxi.fhv_taxi_trips_2019`
where true
  and PUlocationID is null
  and DOlocationID is null
;
```

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

**Answer 4:**

- BQ queries:
```bigquery
/*
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
```

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

**Answer 5:**

- BQ queries:
```bigquery
/*
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
```

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

**Answer 6:**
```
In GCS (GCP Bucket) where it was initially loaded to
```

## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

**Answer 7:**
```
No, in case of extensive Write/Updates it might be inefficient to recluster data every time
```

## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 


Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
 

**Answer 8:**

Data pipeline source code, which uploads parquet files to Google Cloud Storage could be found here: [ETL-flow](./data-pipeline/flow/etl_web_to_gcs.py)

- Execute prefect ETL pipeline to load data from Web to Google Cloud:
```shell
python3 ./data-pipeline/flow/etl_web_to_gcs.py
```

- BigQuery external table:
```bigquery
-- Create external BQ table from parquet files in GCS
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_fhv_taxi_trips_2019`
OPTIONS (
  format="PARQUET",
  uris=[
    'gs://de_zoomcamp_data_lake_hip-plexus-374912/taxi/fhv_tripdata_*.parquet'
  ]
);

-- TEST: format is OK, executes without errors:
select count(*)
from `nytaxi.external_fhv_taxi_trips_2019`
```

## Submitting the solutions

* Form for submitting: https://forms.gle/rLdvQW2igsAT73HTA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 February (Monday), 22:00 CET
