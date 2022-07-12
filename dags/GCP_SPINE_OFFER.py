from airflow import DAG
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCheckOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta


dataset_source = "uk_arc_chordiant_is"
dataset_staging = "uk_pre_customer_spine_offer_is"
dataset_publish = "uk_pub_customer_spine_offer_is"
gs_source_bucket = "uk_customer_tds_is"


default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=60),
    "priority_weight": 10,
    "wait_for_downstream": False,
    "sla": timedelta(seconds=7200),
    "execution_timeout": timedelta(seconds=300),
    "owner": "customerbtprod",
    "email": ["sean.conkie@sky.uk"],
}

with DAG(
    "GCP_SPINE_OFFER",
    concurrency=10,
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False,
    description="Populates portfolio offer tables uk_pub_customer_spine_offer_is",
    tags=["offer", "customer"],
) as dag:

    start_pipeline = DummyOperator(task_id="start_pipeline", dag=dag)

    dim_offer_type = BigQueryOperator(
        task_id="dim_offer_type",
        sql=f"""dags/sql/dim_offer_type.sql""",
        destination_dataset_table=f"""uk_pub_customer_spine_offer_is.dim_offer_type""",
        write_disposition=f"""WRITETRUNCATE""",
        create_disposition=f"""CREATE_IF_NEEDED""",
        allow_large_results=True,
        use_legacy_sql=False,
        params={"dataset_publish": "uk_pub_customer_spine_offer_is"},
        dag=dag,
    )
    dim_offer_type_data_check_row_count = BigQueryCheckOperator(
        task_id="dim_offer_type_data_check_row_count",
        sql=f"""select count(*) from uk_pub_customer_spine_offer_is.dim_offer_type""",
        params={},
        dag=dag,
    )
    dim_offer_type_data_check_duplicate_records = BigQueryCheckOperator(
        task_id="dim_offer_type_data_check_duplicate_records",
        sql=f"""sql/data_check_duplicate_records.sql""",
        params={
            "DATASET_ID": "uk_pub_customer_spine_offer_is",
            "FROM": "dim_offer_type",
            "KEY": "id, offer_detail_id, offer_type_id, created_by_id, created_dt, last_modified_dt, last_modified_by_id, code, description",
        },
        dag=dag,
    )

    finish_pipeline = DummyOperator(task_id="finish_pipeline", trigger_ruke="all_done")

    # Define task dependencies
    start_pipeline >> dim_offer_type
    dim_offer_type >> dim_offer_type_data_check_row_count
    dim_offer_type >> dim_offer_type_data_check_duplicate_records
    dim_offer_type_data_check_row_count >> finish_pipeline
    dim_offer_type_data_check_duplicate_records >> finish_pipeline
