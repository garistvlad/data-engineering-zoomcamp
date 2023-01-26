## Assignment #1. Initial setup

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