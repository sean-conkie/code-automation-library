--------------------------------------------------------------------------------
--
-- filename      : spine_offer_dim_offer_status.sql
-- author        : Ashutosh Ranjan
-- date created  : 28th July 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate dimension table dim_offer_status
--
-- Comments      : NA
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : spine_offer_dim_1.sh
--
-- Calls         : none
--
-- Parameters    : NA
--
-- Exit codes    : 0 - Success
--                 1 - Failure
-- Revisions
-- =============================================================================
-- Date    Userid  MR#         Comments                                     Ver.
-- ------  ------  ----------  -------------------------------------------  ----
-- 280721  ARJ04   cust-spine  Initial Version                              1.0
-- 261021  SGW01   soip        Remove dw_created_dt column                  1.1
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Step 1 - Truncate target table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_status:DELETE:
truncate table uk_pub_customer_spine_offer_is.dim_offer_status; 

--------------------------------------------------------------------------------
-- Step 2 - Do the inserts from the MOBILE/CORE dataset
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_status:WRITE_APPEND:
select portfolio_offer_id,
       current_timestamp() dw_last_modified_dt,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       effective_to_dt,
       status_code,
       status,
       reason_code,
       reason
  from uk_pre_customer_spine_offer_is.td_offer_status;

----------------------------------------------------------------------------------
-- Step 3 - Do the inserts from the SOIP dataset
----------------------------------------------------------------------------------  
uk_pub_customer_spine_offer_is.dim_offer_status:WRITE_APPEND:
select portfolio_offer_id,
       current_timestamp() dw_last_modified_dt,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       effective_to_dt,
       status_code,
       status,
       reason_code,
       reason
  from uk_pre_customer_spine_offer_is.td_offer_status_soip;

  