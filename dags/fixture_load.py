from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

default_args = {
    "owner": "Sean Conkie",
    "email": ["sean.conkie@sky.uk"],
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

project_id = "certain-region-344411"
staging_dataset = "uk_stg_football_is"
datastore_dataset = "uk_tds_football_is"
gs_landing_bucket = "mfp-landing"
gs_loaded_bucket = "mfp-loaded"

with DAG(
    "fixture-load",
    start_date=datetime.now(),
    schedule_interval="@once",
    concurrency=5,
    max_active_runs=1,
    default_args=default_args,
    description="Ingestion of fixture data",
    catchup=False,
    tags=["fixture", "load"],
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline", dag=dag)

    load_fixtures = GoogleCloudStorageToBigQueryOperator(
        task_id="load_fixtures",
        bucket=gs_landing_bucket,
        source_objects=["fixtures*.csv"],
        destination_project_dataset_table=f"{project_id}:{staging_dataset}.ld_fixture",
        schema_object="ld_fixtures.json",
        write_disposition="WRITE_TRUNCATE",
        source_format="json",
        field_delimiter=",",
        skip_leading_rows=1,
    )

    check_fixtures = BigQueryCheckOperator(
        task_id="check_fixtures",
        use_legacy_sql=False,
        sql=f"SELECT count(*) FROM `{project_id}.{staging_dataset}.ld_fixture`",
    )

    loaded_data_to_staging = DummyOperator(task_id="loaded_data_to_staging")

    finish_pipeline = DummyOperator(task_id="finish_pipeline")

    # Define task dependencies
    (
        start_pipeline
        >> load_fixtures
        >> check_fixtures
        >> loaded_data_to_staging
        >> finish_pipeline
    )
