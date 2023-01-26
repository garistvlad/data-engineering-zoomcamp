FROM python:3.10

WORKDIR /app

COPY ./requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app/
COPY .env .env

# docker-compose run pipeline --env='.env' --dataset='data/green_tripdata_2019-01.csv' --tablename='green_trip_data'
# docker-compose run pipeline --env='.env' --dataset='data/taxi_zone_lookup.csv' --tablename='taxi_zone'
ENTRYPOINT ["python3", "pipeline_data_ingestion.py"]
