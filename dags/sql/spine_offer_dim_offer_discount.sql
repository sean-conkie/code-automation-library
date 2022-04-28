--------------------------------------------------------------------------------
--
-- filename      : spine_offer_dim_offer_discount.sql
-- author        : Ashutosh Ranjan
-- date created  : 17th Aug 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate dimension table dim_offer_discount
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
-- 170821  ARJ04   cust-spine  Initial Version                              1.0
-- 081021  cmc37   cust-spine  Added CORE/MOBILE                            1.1
-- 250122  acr73   cust-spine  Modified schema to remove dw_created_dt      1.2
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Step 1 - Truncate target table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.dim_offer_discount:DELETE:
truncate table uk_pub_customer_spine_offer_is.dim_offer_discount; 

----------------------------------------------------------------------------------
-- Step 2 - Do the inserts from the CORE/MOBILE datasets
----------------------------------------------------------------------------------  
uk_pub_customer_spine_offer_is.dim_offer_discount:WRITE_APPEND:
select portfolio_offer_id,
       current_timestamp() dw_last_modified_dt,
       effective_from_dt,
       effective_to_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       original_portfolio_offer_id,
       portfolio_offer_priceable_unit_id,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       currency,
       active_flag,
       status_code,	   
       gross_flag,
       discount_id,
       discount_type,
       discount_priority,
       discount_version,
       discount,
       discount_value
 from  uk_pre_customer_spine_offer_is.td_offer_discount;


----------------------------------------------------------------------------------
-- Step 3 - Do the inserts from the SOIP dataset
----------------------------------------------------------------------------------  
uk_pub_customer_spine_offer_is.dim_offer_discount:WRITE_APPEND:
select portfolio_offer_id,
       current_timestamp() dw_last_modified_dt,
       effective_from_dt,
       effective_to_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       original_portfolio_offer_id,
       portfolio_offer_priceable_unit_id,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       currency,
       active_flag,
       status_code,	   
       gross_flag,
       discount_id,
       discount_type,
       discount_priority,
       discount_version,
       discount,
       discount_value
 from  uk_pre_customer_spine_offer_is.td_offer_discount_soip;

  