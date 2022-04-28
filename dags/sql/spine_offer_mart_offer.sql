--------------------------------------------------------------------------------
--
-- filename      : spine_offer_mart_offer.sql
-- author        : Ankita Roy
-- date created  : 20th July 2022
--
--------------------------------------------------------------------------------
--
-- description   : Population of Spine table MART_OFFER
--
-- comments      : n/a
--
-- usage         : Standard GCP BQ SQL call
--
-- called by     : spine_offer_mart.sh
--
-- calls         : none
--
-- revisions
-- =============================================================================
-- Date     userid  MR#           Comments                                Ver.
-- ------   ------  ------------  --------------------------------------  ----
-- 200222   ary17   Better Data   Initial version                         1.0
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Truncate mart
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.mart_offer:DELETE:
truncate table uk_pub_customer_spine_offer_is.mart_offer;

--------------------------------------------------------------------------------
-- Insert all records into mart table
--------------------------------------------------------------------------------
uk_pub_customer_spine_offer_is.mart_offer:WRITE_APPEND:
select portfolio_offer_id,
       current_timestamp()               dw_last_modified_dt,
       effective_from_dt,
       effective_to_dt,
       created_dt,
       created_by_id,
       history_order,
       discounted_product_event_id,
       portfolio_id,
       billing_account_id,
       account_number,
       account_type,
       currency_code,
       service_instance_id,
       subscription_id,
       discounted_priceable_unit_id,
       discounted_priceable_unit_type_code,
       discounted_priceable_unit_type,
       catalogue_product_id,
       catalogue_product_bill_name,
       catalogue_product_price,
       catalogue_product_name,
       catalogue_product_type,
       catalogue_product_transaction_type,
       entitlement_code,
       order_id,
       previous_portfolio_offer_id,
       first_portfolio_offer_id,
       first_portfolio_offer_order_id,
       first_portfolio_offer_created_by_id,
       first_portfolio_offer_created_dt,
       current_auto_transfer_offer_flag,
       offer_version,
       offer_detail_id,
       offer_code,
       offer_name,
       offer_bill_name,
       offer_transaction_type,
       pac,
       discount_priority,
       discount_type,
       discount,
       discount_value,
       quoted_discount,
       gross_flag,
       automatic_flag,
       status_code,
       status,
       status_reason_code,
       status_reason,
       status_from_dt,
       status_to_dt,
       previous_status,
       previous_status_code,
       previous_status_reason,
       previous_status_reason_code,
       previous_status_from_dt,
       previous_status_to_dt,
       status_changed_flag,
       price_changed_flag,
       active_flag,
       offer_duration,
       offer_start_dt,
       offer_end_dt,
       activation_dt,
       terminated_dt
  from uk_pre_customer_spine_offer_is.td_mart_offer;

