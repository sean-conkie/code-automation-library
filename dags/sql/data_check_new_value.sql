--------------------------------------------------------------------------------
--
-- Filename      : data_check_duplicate_records.sql
-- Author        : Sean Conkie
-- Date Created  : 10 May 2022
--
--------------------------------------------------------------------------------
--
-- Description   : Template .sql for use by BigQueryCheckOperator tasks in 
--                 Airflow to check if new values have been included in the 
--                 supplied field.
--
-- Comments      : NA
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : Airflow task
--
-- Calls         : none.
--
-- Parameters    : 1) lower_date_bound - DD-MON-YYYY HH24:MI:SS
--                 2) upper_date_bound - DD-MON-YYYY HH24:MI:SS
--
-- Exit codes    : 0 - Success
--                 1 - Failure
--
-- Revisions
-- =============================================================================
-- Date     userid  MR#       Comments                                      Ver.
-- ------   ------  ------    --------------------------------------------  ----
-- 100522   sci07             Initial version                               1.0
--------------------------------------------------------------------------------

with
     query_date as (select date(date_add(max({{ params.DATE_FIELD }}), interval -1 day)) query_date
                      from {{ params.DATASET_ID }}.{{ params.FROM }})
                     where effective_from_dt < current_timestamp()),
     old_codes as (select distinct od.{{ params.CHECK_FIELD }}
                     from {{ params.DATASET_ID }}.{{ params.FROM }}) od
                    inner join query_date qd
                       on date(od.effective_from_dt) <= qd.query_date),
     new_codes as (select distinct nd.{{ params.CHECK_FIELD }}
                     from {{ params.DATASET_ID }}.{{ params.FROM }}) nd
                    inner join query_date qd
                       on date(nd.effective_from_dt) > qd.query_date)

select if(count(*) > 0, false, true)
  from new_codes nc
  left join old_codes oc
    on nc.{{ params.CHECK_FIELD }} = oc.{{ params.CHECK_FIELD }}
  where oc.status_code is null;
