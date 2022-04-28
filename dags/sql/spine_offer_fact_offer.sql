--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_fact_offer.sql
-- Author        : Emanuel Blei
-- Date Created  : 05th Aug 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate FACT_OFFER
--
-- Comments      : 2nd phase version
--
-- Usage         : GCP Big query
--
-- Called By     : spine_offer_fact.sh
--
-- Calls         : none.
--
-- Parameters    : n/a
--
-- Exit codes    : 0 - Success
--                 1 - Failure
--
-- Revisions
-- =============================================================================
-- Date     userid   MR#      Comments                                      Ver.
-- ------   ------   ------   --------------------------------------------  ----
-- 050821   ebi08    BQ       Initial Version for GCP Big Query              1.0
-- 190821   ebi08    BQ       Added CORE/MOBILE table                        1.1
-- 240122   ary17    BQ       Added first_portfolio_offer_id                 1.2
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- Step 1 - Truncate the target table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.fact_offer:DELETE:
truncate table uk_pub_customer_spine_offer_is.fact_offer;

--------------------------------------------------------------------------------
-- Step 2 - Insert CORE/MOBILE records into target table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.fact_offer:WRITE_APPEND:
select id,
       current_timestamp() dw_last_modified_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       portfolio_id,
       billing_account_id,
       account_number,
       service_instance_id,
       account_type,
       currency_code,
       order_id,
       order_line_id,
       previous_portfolio_offer_id,
       first_portfolio_offer_id,
       discount_id,
       discount_type,
       discount_duration,
       discount,
       start_dt,
       end_dt,
       order_urn,
       offer_detail_id,
       code,
       description,
       name,
       bill_name,
       transaction_type,
       pac,
       version,
       active_flag,
       automatic_flag,
       gross_flag,
       global_flag,
       price_freeze_offer_flag,
       in_contract_offer_flag
  from uk_pre_customer_spine_offer_is.td_fact_offer;

--------------------------------------------------------------------------------
-- Step 3 - Insert SOIP records into target table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.fact_offer:WRITE_APPEND:
select id,
       current_timestamp() dw_last_modified_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       portfolio_id,
       billing_account_id,
       account_number,
       service_instance_id,
       account_type,
       currency_code,
       order_id,
       order_line_id,
       previous_portfolio_offer_id,
       first_portfolio_offer_id,
       discount_id,
       discount_type,
       discount_duration,
       discount,
       start_dt,
       end_dt,
       order_urn,
       offer_detail_id,
       code,
       description,
       name,
       bill_name,
       transaction_type,
       pac,
       version,
       active_flag,
       automatic_flag,
       gross_flag,
       global_flag,
       price_freeze_offer_flag,
       in_contract_offer_flag
  from uk_pre_customer_spine_offer_is.td_fact_offer_soip;
