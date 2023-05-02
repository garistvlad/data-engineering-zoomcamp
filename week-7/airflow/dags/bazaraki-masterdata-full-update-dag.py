from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from docker.types import Mount

# TODO: fix hardcoded paths to .env / config.yaml
LOCAL_AIRFLOW_DIR = Path("/Users/vgarist/Education/data-engineering-zoomcamp/home-assignments/week-7/airflow")
DOCKER_AIRFLOW_DIR = Path("/opt/airflow")
RAW_DATA_DIR = Path(DOCKER_AIRFLOW_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = Path(DOCKER_AIRFLOW_DIR, 'data', 'processed')

FS_CONNECTIO_ID = "local-file-system"
GCP_CONNECTION_ID = "gcp-de-zoomcamp-sa"
GCP_PROJECT_ID = "hip-plexus-374912"


docker_default_params = {
    "image": 'garistvlad/bazaraki-parser',
    "container_name": 'task___bazaraki-parser-masterdata',
    "mount_tmp_dir": False,
    "auto_remove": "force",
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "bridge",
    "mounts": [
        Mount(
            source=str(Path(LOCAL_AIRFLOW_DIR, "data")),
            target=str(Path(DOCKER_AIRFLOW_DIR, "data")),
            type="bind"
        ),
    ]
}


with DAG(
        "bararaki-parser-load-masterdata",
        default_args={
            "depends_on_past": True,
            "email": ["vv.garist@gmail.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=10),
        },
        description="Parse Bazaraki masterdata: cities, districts, realty rubrics",
        start_date=datetime(2023, 1, 1),
        schedule_interval="@once",
        catchup=False,
        tags=["bazaraki", "de-zoomcamp", "cy-properties"]
):

    t1 = DockerOperator(
        task_id='parser-load-masterdata',
        command=f"""
        python3 src/etl_load_masterdata_from_web_to_parquet.py \
            --pipeline_params_filepath=configs/pipeline_params.yaml
         """,
        **docker_default_params
    )
    t2 = FileSensor(
        task_id="check-raw-data-exists",
        filepath=RAW_DATA_DIR,
        fs_conn_id=FS_CONNECTIO_ID
    )

    t3 = DockerOperator(
        task_id='parser-process-rubrics-and-features',
        command=f"""
        python3 src/etl_process_rubrics_and_features.py \
            --pipeline_params_filepath=configs/pipeline_params.yaml
        """,
        **docker_default_params
    )
    t4 = FileSensor(
        task_id="check-processed-data-exists",
        filepath=PROCESSED_DATA_DIR,
        fs_conn_id=FS_CONNECTIO_ID
    )

    t5 = DockerOperator(
        task_id='parser-process-cities-and-districts',
        command=f"""
            python3 src/etl_process_cities_and_districts.py \
                --pipeline_params_filepath=configs/pipeline_params.yaml
            """,
        **docker_default_params
    )

    t6 = LocalFilesystemToGCSOperator(
        task_id="load-parquet-masterdata-to-gcp",
        src=[
            str(Path(PROCESSED_DATA_DIR, "cities.parquet")),
            str(Path(PROCESSED_DATA_DIR, "districts.parquet")),
            str(Path(PROCESSED_DATA_DIR, "rubric_features.parquet")),
            str(Path(PROCESSED_DATA_DIR, "rubrics.parquet")),
        ],
        dst="parsed-from-bazaraki/masterdata/",
        bucket="bazaraki-bucket",
        gcp_conn_id=GCP_CONNECTION_ID,
        gzip=False,
    )

    t7 = list()
    for task_id, table_name, bq_filename in zip(
        ["bq-create-external-city", "bq-create-external-district", "bq-create-external-rubric", "bq-create-external-rubric-feature"],
        ["bazaraki.external_city", "bazaraki.external_district", "bazaraki.external_rubric", "bazaraki.external_rubric_feature"],
        ["cities.parquet", "districts.parquet", "rubrics.parquet", "rubric_features.parquet"],
    ):
        t7.append(
            BigQueryExecuteQueryOperator(
                task_id=task_id,
                sql=f"""
                CREATE OR REPLACE EXTERNAL TABLE `{table_name}`
                OPTIONS (
                    format="PARQUET",
                    uris=[
                    'gs://bazaraki-bucket/parsed-from-bazaraki/masterdata/{bq_filename}'
                    ]
                )
                """,
                use_legacy_sql=False,
                allow_large_results=True,
                gcp_conn_id=GCP_CONNECTION_ID,
            )
        )

    t8 = DockerOperator(
        task_id='dbt-create-stg-tables',
        command=f"dbt run --project-dir bazaraki --select stg_city stg_district stg_rubric stg_rubric_feature",
        image='garistvlad/bazaraki-dbt',
        container_name='task___bazaraki-dbt-masterdata',
        mount_tmp_dir=False,
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source=str(Path(LOCAL_AIRFLOW_DIR.parent, "dbt")),
                target="/usr/app/dbt/",
                type="bind"
            ),
        ]
    )

    t9 = list()
    for table_name in ["stg_city", "stg_district", "stg_rubric", "stg_rubric_feature"]:
        t9.append(
            BigQueryTableExistenceSensor(
                task_id=f"bq-check-{table_name}-created",
                project_id=GCP_PROJECT_ID,
                dataset_id="bazaraki",
                table_id=table_name,
                gcp_conn_id=GCP_CONNECTION_ID,
            )
        )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
