### Initial setup

- Create `.env` file (use `.env.example` as a template) inside root directory. Format us as follows:
    ```shell
    # postgres credentials:
    POSTGRES_USER=...
    POSTGRES_PASSWORD=...
    POSTGRES_HOST=...
    POSTGRES_PORT=...
    POSTGRES_DB=...
    ```

- Create the same `.env` file (use `.env.example` as a template) inside `data_pipeline/` directory. It is needed to lauch docker container with `data_pipeline.py` ingestion script.


- Start docker rcompose with Postgres, PgAdmin and an Entrypoint ready for data loading to Postgres:
    ```
    docker-compose up -d --build  
    ```
  
- Load data to Postgres from CSV files:
    ```
    docker-compose run pipeline \
        --env='.env' \
        --dataset='data/green_tripdata_2019-01.csv' \
        --tablename='green_trip_data'
    
    docker-compose run pipeline \
        --env='.env' \
        --dataset='data/taxi_zone_lookup.csv' \
        --tablename='taxi_zone'
    ```
    
### My solution for [Assignment #1](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md)

1. 
```shell
docker build --help
```

```shell
...
--iidfile string          Write the image ID to the file
...
```

2. 
```shell
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```

3. 
```sql
select count(*) as trip_cnt
from green_trip_data
where true
    and lpep_pickup_datetime::date = '2019-01-15'::date
    and lpep_dropoff_datetime::date = '2019-01-15'::date
```

4.
```sql
select lpep_pickup_datetime::date as d_date,
       max(trip_distance) as max_tip_distance
from green_trip_data
group by d_date
order by max_tip_distance desc
limit 1
```

5.
```sql
select passenger_count,
       count(*) as trip_cnt
from green_trip_data
where true
    and lpep_pickup_datetime::date = '2019-01-01'::date
    and passenger_count in (2,3)
group by 1
order by 1
```

6.
```sql
select
    gtd.pulocationid,
    pu_tz.zone as pu_zone,
    gtd.dolocationid,
    do_tz.zone as do_zone,
    gtd.tip_amount
from green_trip_data gtd
join taxi_zone do_tz
    on gtd.dolocationid = do_tz.locationid
join taxi_zone pu_tz
    on gtd.pulocationid = pu_tz.locationid
where true
    and pu_tz.zone = 'Astoria'
order by tip_amount desc
limit 1
```

7.
```shell
google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/hip-plexus-374912/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 1s [id=de_zoomcamp_data_lake_hip-plexus-374912]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
```
