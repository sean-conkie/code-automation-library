--------------------------------------------------------------------------------
--
-- Filename      : spine_dim_offer_type.sql
-- Author        : Sandeep Bibekar
-- Date Created  : 19th Jul 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate DIM_OFFER_TYPE
--
-- Comments      : Initial Version
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : spine_offer_dim_1.sh
--
-- Calls         : none.
--
-- Parameters    : n/a
--
-- Exit codes    : 0 - Success
--                 1 - Failure
--
-- =============================================================================
-- Date     userid  MR#       Comments                                      Ver.
-- ------   ------  ------    --------------------------------------------  ----
-- 190721   sbr55   Spine     initial version                               1.0
-- 260721   abr87   Spine     updated to coding standards                   1.1
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Delete for restartability
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_type:DELETE:
truncate table uk_pub_customer_spine_offer_is.dim_offer_type;

--------------------------------------------------------------------------------
-- Step 1 - Populate Populate DIM_OFFER_TYPE
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_type:WRITE_APPEND:
select id,
       offer_detail_id,
       offer_type_id,
       current_timestamp()    dw_created_dt,
       current_timestamp()    dw_last_modified_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       code,
       description
  from uk_pre_customer_spine_offer_is.td_offer_type;
