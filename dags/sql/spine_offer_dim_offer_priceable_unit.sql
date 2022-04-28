--------------------------------------------------------------------------------
--
-- Filename      : spine_dim_offer_priceable_unit.sql
-- Author        : Ankita Gaikar
-- Date Created  : 3rd August 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate DIM_OFFER_PRICEABLE_UNIT
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
-- =============================================================================
-- Date     userid  MR#       Comments                                      Ver.
-- ------   ------  ------    --------------------------------------------  ----
-- 030821   agg01   BQ        Initial version                               1.0
-- 260821   ebi08   BQ        Added quoted_discount column                  1.1
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Step 1 - Truncate target table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_priceable_unit:DELETE:
truncate table uk_pub_customer_spine_offer_is.dim_offer_priceable_unit;

--------------------------------------------------------------------------------
-- Step 2 - Do the inserts from the MOBILE/CORE dataset
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_priceable_unit:WRITE_APPEND:
select id,
       current_timestamp() dw_created_dt,
       current_timestamp() dw_last_modified_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       portfolio_offer_id,
       discounted_priceable_unit_id,
       effective_dt,
       end_dt,
       quoted_discount
  from uk_pre_customer_spine_offer_is.td_offer_priceable_unit;

----------------------------------------------------------------------------------
-- Step 3 - Do the inserts from the SOIP dataset
---------------------------------------------------------------------------------- 
uk_pub_customer_spine_offer_is.dim_offer_priceable_unit:WRITE_APPEND:
select id,
       current_timestamp() dw_created_dt,
       current_timestamp() dw_last_modified_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       portfolio_offer_id,
       discounted_priceable_unit_id,
       effective_dt,
       end_dt,
       quoted_discount
  from uk_pre_customer_spine_offer_is.td_offer_priceable_unit_soip;
  