
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator



dataset_staging = 'uk_pre_customer_spine_offer_is'
dataset_publish = 'uk_pub_customer_spine_offer_is'


default_args = {'owner': 'customerbtprod','email': ['sean.conkie@sky.uk'],'depends_on_past': False,'email_on_failure': False,'email_on_retry': False,'retries': '5','retry_delay': timedelta(seconds=60),'priority_weight': '10','wait_for_downstream': False,'sla': timedelta(seconds=7200),'execution_timeout': timedelta(seconds=300)}

with DAG('GCP_SPINE_OFFER',
          concurrency=5,
          max_active_runs=1,
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime.now(),
          description='Populates portfolio offer tables uk_pub_customer_spine_offer_is',
          catchup=False,
          tags=['offer', 'customer']) as dag:


    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
    )

    dim_offer_type = BigQueryOperator(task_id='dim_offer_type',
          create_disposition='CREATE_IF_NEEDED',
          allow_large_results=True,
          use_legacy_sql=False,
          sql=f"""truncate table {dataset_publish}.dim_offer_type;
insert into {dataset_publish}.dim_offer_type
select a.id,
       current_timestamp()                                 dw_last_modified_dt,
       a.offerid                                           offer_detail_id,
       a.offertypeid                                       offer_type_id,
       a.offertypeid                                       offer_type_id,
       a.created                                           created_dt,
       a.createdby                                         created_by_id,
       a.lastupdate                                        last_modified_dt,
       a.updatedby                                         last_modified_by_id,
       b.code,
       b.description
  from uk_tds_refdata_eod_is.cc_refdata_bsboffertooffertype a
  left join uk_tds_refdata_eod_is.cc_refdata_bsboffertype b
    on (    a.offertypeid = b.id
        and b.rdmaction <> 'D')
 where a.offertypeid <> 'D'
;
""",
          destination_dataset_table=f'{dataset_publish}.dim_offer_type',
          write_disposition='WRITE_TRUNCATE',
          dag=dag)

    td_offer_status_core = BigQueryOperator(task_id='td_offer_status_core',
          create_disposition='CREATE_IF_NEEDED',
          allow_large_results=True,
          use_legacy_sql=False,
          sql=f"""create or replace table uk_pre_customer_spine_offer_is.td_offer_status_core_p1 as
select a.id                                                portfolio_offer_id,
       current_timestamp()                                 dw_last_modified_dt,
       ifnull(a.statuschangeddate,a.effective_from_dt)     effective_from_dt,
       a.effective_from_dt_csn_seq,
       a.effective_from_dt_seq,
       a.statuschangeddate                                 effective_to_dt,
       a.status                                            status_code,
       b.codedesc                                          status,
       a.statusreasoncode                                  reason_code,
       b.codedesc                                          reason,
       lag(a.status,1) over(partition by a.id order by ifnull(a.statuschangeddate,a.effective_from_dt),a.effective_from_dt_csn_seq,a.effective_from_dt_seq) prev_status_code,
       lag(a.statusreasoncode,1) over(partition by a.id order by ifnull(a.statuschangeddate,a.effective_from_dt),a.effective_from_dt_csn_seq,a.effective_from_dt_seq) prev_reason_code
  from uk_tds_chordiant_eod_fc_is.fc_chordiant_bsbportfoliooffer a
  left join uk_tds_chordiant_eod_is.cc_chordiant_picklist b
    on (    a.status = b.code
        and b.logically_deleted = 0)
 where a.logically_deleted = 0
;

create or replace table uk_pre_customer_spine_offer_is.td_offer_status_core_p2 as
select * except(prev_status_code,prev_reason_code)
  from uk_pre_customer_spine_offer_is.td_offer_status_core_p1 c
 where ifnull(cast(status_code as string),'NULL') <> ifnull(cast(prev_status_code as string),'NULL')
   and ifnull(cast(reason_code as string),'NULL') <> ifnull(cast(prev_reason_code as string),'NULL')
;

truncate table uk_pre_customer_spine_offer_is.td_offer_status_core;
insert into uk_pre_customer_spine_offer_is.td_offer_status_core
select d.portfolio_offer_id,
       d.dw_last_modified_dt,
       d.effective_from_dt,
       d.effective_from_dt_csn_seq,
       d.effective_from_dt_seq,
       lead(effective_from_dt,1,timestamp('2999-12-31 23:59:59')) over(partition by  portfolio_offer_id order by effective_from_dt,effective_from_dt_csn_seq,effective_from_dt_seq) effective_to_dt,
       d.status_code,
       d.status,
       d.reason_code,
       d.reason
  from uk_pre_customer_spine_offer_is.td_offer_status_core_p2 d
;
""",
          destination_dataset_table=f'uk_pre_customer_spine_offer_is.td_offer_status_core',
          write_disposition='WRITE_TRUNCATE',
          dag=dag)

    td_offer_priceable_unit_core = BigQueryOperator(task_id='td_offer_priceable_unit_core',
          create_disposition='CREATE_IF_NEEDED',
          allow_large_results=True,
          use_legacy_sql=False,
          sql=f"""truncate table uk_pre_customer_spine_offer_is.td_offer_priceable_unit_core;
insert into uk_pre_customer_spine_offer_is.td_offer_priceable_unit_core
select a.id,
       current_timestamp()                                 dw_last_modified_dt,
       a.created                                           created_dt,
       a.createdby                                         created_by_id,
       a.lastupdate                                        last_modified_dt,
       a.updatedby                                         last_modified_by_id,
       a.portfolioofferid                                  portfolio_offer_id,
       a.discountedpriceableunitid                         discounted_priceable_unit_id,
       a.effectivedate                                     effective_dt,
       a.endate                                            end_dt,
       abs(a.quotedprice)                                  quoted_discount
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbpriceableunit a
 where a.logically_deleted = 0
;
""",
          destination_dataset_table=f'uk_pre_customer_spine_offer_is.td_offer_priceable_unit_core',
          write_disposition='WRITE_TRUNCATE',
          dag=dag)

    td_offer_priceable_unit_soip = BigQueryOperator(task_id='td_offer_priceable_unit_soip',
          sql = 'sql/spine_offer_td_offer_priceable_unit_soip.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    truncate_dim_offer_priceable_unit = BigQueryOperator(task_id='truncate_dim_offer_priceable_unit',
          sql = 'truncate table uk_pub_customer_spine_offer_is.dim_offer_priceable_unit;',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    dim_offer_priceable_unit_core = BigQueryOperator(task_id='dim_offer_priceable_unit_core',
          create_disposition='CREATE_IF_NEEDED',
          allow_large_results=True,
          use_legacy_sql=False,
          sql=f"""insert into {dataset_publish}.dim_offer_priceable_unit
select a.id,
       a.dw_last_modified_dt,
       a.created_dt,
       a.created_by_id,
       a.last_modified_dt,
       a.last_modified_by_id,
       a.portfolio_offer_id,
       a.discounted_priceable_unit_id,
       a.effective_dt,
       a.end_dt,
       a.quoted_discount
  from uk_pre_customer_spine_offer_is.td_offer_priceable_unit_core a

;
""",
          destination_dataset_table=f'{dataset_publish}.dim_offer_priceable_unit',
          write_disposition='WRITE_APPEND',
          dag=dag)

    dim_offer_priceable_unit_soip = BigQueryOperator(task_id='dim_offer_priceable_unit_soip',
          create_disposition='CREATE_IF_NEEDED',
          allow_large_results=True,
          use_legacy_sql=False,
          sql=f"""insert into {dataset_publish}.dim_offer_priceable_unit
select a.id,
       a.dw_last_modified_dt,
       a.created_dt,
       a.created_by_id,
       a.last_modified_dt,
       a.last_modified_by_id,
       a.portfolio_offer_id,
       a.discounted_priceable_unit_id,
       a.effective_dt,
       a.end_dt,
       a.quoted_discount
  from uk_pre_customer_spine_offer_is.td_offer_priceable_unit_soip a

;
""",
          destination_dataset_table=f'{dataset_publish}.dim_offer_priceable_unit',
          write_disposition='WRITE_APPEND',
          dag=dag)

    td_offer_discount_core_pt1 = BigQueryOperator(task_id='td_offer_discount_core_pt1',
          sql = 'sql/spine_offer_td_offer_discount_1.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    td_offer_discount_core_pt2 = BigQueryOperator(task_id='td_offer_discount_core_pt2',
          sql = 'sql/spine_offer_td_offer_discount_2.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    td_offer_discount_soip = BigQueryOperator(task_id='td_offer_discount_soip',
          sql = 'sql/spine_offer_td_offer_discount_soip.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    dim_offer_discount = BigQueryOperator(task_id='dim_offer_discount',
          sql = 'sql/spine_offer_dim_offer_discount.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    td_offer_status_soip = BigQueryOperator(task_id='td_offer_status_soip',
          sql = 'sql/spine_offer_td_offer_status_soip.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    dim_offer_status = BigQueryOperator(task_id='dim_offer_status',
          sql = 'sql/spine_offer_dim_offer_status.sql',
          destination_dataset_table = '',
          write_disposition = 'WRITE_TRUNCATE',
          create_disposition = 'CREATE_IF_NEEDED',
          allow_large_results = True,
          use_legacy_sql = False,
          dag=dag)

    

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline'
    )

  # Define task dependencies
    start_pipeline >> dim_offer_type
    start_pipeline >> td_offer_status_core
    start_pipeline >> td_offer_priceable_unit_core
    start_pipeline >> td_offer_priceable_unit_soip
    td_offer_priceable_unit_soip >> truncate_dim_offer_priceable_unit
    td_offer_priceable_unit_core >> truncate_dim_offer_priceable_unit
    truncate_dim_offer_priceable_unit >> dim_offer_priceable_unit_core
    truncate_dim_offer_priceable_unit >> dim_offer_priceable_unit_soip
    start_pipeline >> td_offer_discount_core_pt1
    td_offer_discount_core_pt1 >> td_offer_discount_core_pt2
    start_pipeline >> td_offer_discount_soip
    td_offer_discount_core_pt2 >> dim_offer_discount
    td_offer_discount_soip >> dim_offer_discount
    start_pipeline >> td_offer_status_soip
    td_offer_status_core >> dim_offer_status
    td_offer_status_soip >> dim_offer_status
    dim_offer_type >> finish_pipeline
    dim_offer_priceable_unit_core >> finish_pipeline
    dim_offer_priceable_unit_soip >> finish_pipeline
    dim_offer_discount >> finish_pipeline
    dim_offer_status >> finish_pipeline
    