## Week 4 Homework 

Related artefacts:
- ETL pipeline to load data from WEB to Google Cloud Storage: [etl_web_to_gcs.py](./data-pipeline/flow/etl_web_to_gcs.py)
- DBT project with all the models and seeds: [DBT-project](./dbt-projects)

### Question 1:

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)** 

You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41,648,442
- 51,648,442
- **61,648,442**
- 71,648,442

**Answer 1:**

- Related SQL query:
```bigquery
-- 61,648,442
SELECT count(*)
FROM `hip-plexus-374912.dbt_models.fact_trips`
WHERE true
  AND date(pickup_datetime) between '2019-01-01' and '2020-12-31'
```

### Question 2:

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

- **89.9/10.1**
- 94/6
- 76.3/23.7
- 99.1/0.9

**Answer 2:**
- my result: `Yellow 89.4%` vs `Green 10.6%`

### Question 3:

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33,244,696
- **43,244,696**
- 53,244,696
- 63,244,696

**Answer 3:**

- DBT model definition could be found here: [stg_fhv_trips.sql](./dbt-projects/ny_taxi/models/staging/stg_fhv_trips.sql)
- related SQL query:

```bigquery
-- 43,244,696 rows
SELECT count(*) as record_count
FROM `hip-plexus-374912.dbt_models.stg_fhv_trips`
```


### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12,998,722
- **22,998,722**
- 32,998,722
- 42,998,722

**Answer 4:**

- DBT model definition could be found here: []
- related SQL query:

```bigquery
-- 22,998,722
SELECT 
  count(*) as record_count
FROM `hip-plexus-374912.dbt_models.fact_fhv_trips`
WHERE true
  AND date(pickup_datetime) between '2019-01-01' and '2020-12-31'
```

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- **January**
- December

**Answer 5:**

- Example of visualization from Data-Studio:

- Related SQL query as a double check:
```bigquery
-- January
SELECT 
  extract(month from pickup_datetime) as d_month,
  count(*)
FROM `hip-plexus-374912.dbt_models.fact_fhv_trips`
WHERE true
  AND date(pickup_datetime) between '2019-01-01' and '2020-12-31'
group by 1
order by 2 desc
```
