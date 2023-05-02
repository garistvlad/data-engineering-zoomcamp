from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.filesystem import FileSensor

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from docker.types import Mount

LOCAL_AIRFLOW_DIR = Path("/Users/vgarist/Education/data-engineering-zoomcamp/home-assignments/week-7/airflow")
DOCKER_AIRFLOW_DIR = Path("/opt/airflow")
RAW_DATA_DIR = Path(DOCKER_AIRFLOW_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = Path(DOCKER_AIRFLOW_DIR, 'data', 'processed')

FS_CONNECTIO_ID = "local-file-system"
GCP_CONNECTION_ID = "gcp-de-zoomcamp-sa"
GCP_PROJECT_ID = "hip-plexus-374912"


docker_default_params = {
    "image": 'garistvlad/bazaraki-parser',
    "container_name": 'task___bazaraki-parser-full',
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
        "bararaki-parser-load-advertisements-full",
        default_args={
            "depends_on_past": True,
            "email": ["vv.garist@gmail.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=10),
        },
        description="Parse Bazaraki advertisements: full update",
        start_date=datetime.now() - timedelta(days=8),
        schedule_interval="0 0 * * 6",  # At 00:00 on Saturday
        catchup=False,
        tags=["bazaraki", "de-zoomcamp", "cy-properties"]
):

    t1 = DockerOperator(
        task_id='parser-load-advertisements-full',
        command=f"""
        python3 src/etl_load_ads_from_web_to_parquet.py \
            --pipeline_params_filepath=configs/pipeline_params.yaml \
            --full
        """,
        **docker_default_params
    )
    t2 = FileSensor(
        task_id="check-raw-data-exists",
        filepath=Path(RAW_DATA_DIR, "ads-full"),
        fs_conn_id=FS_CONNECTIO_ID
    )

    t3 = DockerOperator(
        task_id='parser-process-advertisements-full',
        command=f"""
        python3 src/etl_process_advertisements.py \
            --pipeline_params_filepath=configs/pipeline_params.yaml \
            --full
        """,
        **docker_default_params
    )
    t4 = FileSensor(
        task_id="check-processed-data-exists",
        filepath=Path(PROCESSED_DATA_DIR, "ads-full"),
        fs_conn_id=FS_CONNECTIO_ID
    )

    t5 = LocalFilesystemToGCSOperator(
        task_id="load-parquet-ads-to-gcp",
        src=f"{Path(PROCESSED_DATA_DIR, 'ads-full')}/*.parquet",
        dst="parsed-from-bazaraki/ads-full/",
        bucket="bazaraki-bucket",
        gcp_conn_id=GCP_CONNECTION_ID,
        gzip=False,
    )

    t6 = list()
    for parquet_filename, table_name in [
        ("advertisements.parquet", "bazaraki.external_advertisement_full"),
        ("image_to_advertisement_mapping.parquet", "bazaraki.external_image_to_advertisement_mapping_full"),
        ("images.parquet", "bazaraki.external_image_full"),
        ("users.parquet", "bazaraki.external_user_full")
    ]:
        t6.append(
            BigQueryExecuteQueryOperator(
                task_id=f"bq-create-{table_name}",
                sql=f"""
                CREATE OR REPLACE EXTERNAL TABLE `{table_name}`
                OPTIONS (
                    format="PARQUET",
                    uris=[
                    'gs://bazaraki-bucket/parsed-from-bazaraki/ads-full/{parquet_filename}'
                    ]
                )
                """,
                use_legacy_sql=False,
                allow_large_results=True,
                gcp_conn_id=GCP_CONNECTION_ID,
            )
        )

    t7 = DockerOperator(
        task_id='dbt-create-stg-tables',
        command=f"""
        dbt run --project-dir bazaraki \
        --select stg_advertisement_full stg_image_to_advertisement_mapping_full \
            stg_image_full stg_user_full
        """,
        image='garistvlad/bazaraki-dbt',
        container_name='task___bazaraki-dbt-full',
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

    t8 = BigQueryTableExistenceSensor(
        task_id="bq-check-table-created",
        project_id=GCP_PROJECT_ID,
        dataset_id="bazaraki",
        table_id="stg_advertisement_full",
        gcp_conn_id=GCP_CONNECTION_ID,
    )

    t9 = DockerOperator(
        task_id='dbt-update-core-tables',
        command=f"""
        dbt run \
        --project-dir bazaraki \
        --select core_advertisement core_user \
            core_image_to_advertisement_mapping core_image
        """,
        image='garistvlad/bazaraki-dbt',
        container_name='task___bazaraki-dbt-full',
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

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
