from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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
archive_dataset = "uk_arc_football_is"
datastore_dataset = "uk_tds_football_is"
gs_landing_bucket = "mfp-load"
gs_loaded_bucket = "mfp-loaded"

with DAG(
    "league-load",
    start_date=datetime.now(),
    schedule_interval=None,
    concurrency=5,
    max_active_runs=1,
    default_args=default_args,
    description="Ingestion of league data",
    catchup=False,
    tags=["league", "load"],
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline", dag=dag)

    load_leagues = GoogleCloudStorageToBigQueryOperator(
        task_id="load_leagues",
        bucket=gs_landing_bucket,
        #   source_objects = [{{ dag_run.conf["name"] }} ],
        source_objects=["league*.json"],
        destination_project_dataset_table=f"{project_id}:{archive_dataset}.arc_league",
        schema_object="arc_league.json",
        write_disposition="WRITE_APPEND",
        source_format="NEWLINE_DELIMITED_JSON",
        field_delimiter=",",
        skip_leading_rows=1,
    )

    check_leagues = BigQueryCheckOperator(
        task_id="check_leagues",
        use_legacy_sql=False,
        sql=f"SELECT count(*) FROM `{project_id}.{archive_dataset}.arc_league`",
    )

    #   copy_single_file = GCSToGCSOperator(
    #     task_id="copy_single_gcs_file",
    #     source_bucket=BUCKET_1_SRC,
    #     source_object=OBJECT_1,
    #     destination_bucket=BUCKET_1_DST,  # If not supplied the source_bucket value will be used
    #     destination_object="backup_" + OBJECT_1,  # If not supplied the source_object value will be used
    #   )

    loaded_data_to_archive = DummyOperator(task_id="loaded_data_to_archive")

    build_cc_league = BigQueryOperator(
        task_id="build_cc_league",
        sql="sql/build_cc.sql",
        destination_dataset_table=f"{project_id}:{datastore_dataset}.cc_league",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        allow_large_results=True,
        use_legacy_sql=False,
        params={
            "PROJECT_NAME": project_id,
            "DATASET_ID": archive_dataset,
            "FROM": "arc_league, unnest(parameters) params",
            "KEY": "params.season",
            "EXCLUDE": ",season",
        },
    )

    loaded_data_to_datastore = DummyOperator(task_id="loaded_data_to_datastore")

    finish_pipeline = DummyOperator(task_id="finish_pipeline")

    # Define task dependencies
    (
        start_pipeline
        >> load_leagues
        >> check_leagues
        >> loaded_data_to_archive
        >> [build_cc_league]
        >> loaded_data_to_datastore
        >> finish_pipeline
    )
