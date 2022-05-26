--------------------------------------------------------------------------------
--
-- Filename      : data_check_duplicate_records.sql
-- Author        : Sean Conkie
-- Date Created  : 10 May 2022
--
--------------------------------------------------------------------------------
--
-- Description   : Template .sql for use by BigQueryCheckOperator tasks in 
--                 Airflow to validate tables have only one record per PK.
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
     duplicates as (select {{ params.KEY }}
                      from {{ params.DATASET_ID }}.{{ params.FROM }})
                    group by {{ params.KEY }}
                    having count(1) > 1)

select if(count(*) > 0, false, true) result
  from duplicates;
