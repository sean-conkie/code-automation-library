--------------------------------------------------------------------------------
--
-- Filename      : spine_td_offer_priceable_unit.sql
-- Author        : Ankita Gaikar
-- Date Created  : 3rd August 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate TD_OFFER_PRICEABLE_UNIT as a precursor to populating
--                 DIM_OFFER_PRICEABLE_UNIT
--
-- Comments      : NA
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : spine_order_dim_1.sh
--
-- Calls         : none.
--
-- Parameters    : NA
--
-- Exit codes    : 0 - Success
--                 1 - Failure
-- =============================================================================
-- Date     userid  MR#       Comments                                      Ver.
-- ------   ------  ------    --------------------------------------------  ----
-- 030821   agg01   BQ        Initial version                               1.0
-- 260821   ebi08   BQ        Added quoted_discount column                  1.1
--------------------------------------------------------------------------------
-- =============================================================================
-- Get sub-set of data from driving table CC_CHORDIANT_BSBPRICEABLEUNIT
-- Take absolute value of quoted price to calculate quoted_discount
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_priceable_unit_p1:WRITE_TRUNCATE:
select bpa.id                                         id,
       bpa.created                                    created_dt,
       bpa.createdby                                  created_by_id,
       bpa.lastupdate                                 last_modified_dt,
       bpa.updatedby                                  last_modified_by_id,
       bpa.portfolioofferid                           portfolio_offer_id,
       bpa.discountedpriceableunitid                  discounted_priceable_unit_id,
       bpa.effectivedate                              effective_dt,
       bpa.enddate                                    end_dt,
       abs(bpa.quotedprice)                           quoted_discount
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbpriceableunit bpa
 where bpa.priceableunittypecode = 'OF'
   and bpa.portfolioofferid is not null
   and bpa.logically_deleted = 0;

----------------------------------------------------------------------------------------------------------
-- Write_truncate Insert data from SPINE to td table
----------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_priceable_unit:WRITE_TRUNCATE:
select p1.id,
       p1.created_dt,
       p1.created_by_id,
       p1.last_modified_dt,
       p1.last_modified_by_id,
       p1.portfolio_offer_id,
       p1.discounted_priceable_unit_id,
       p1.effective_dt,
       p1.end_dt,
       p1.quoted_discount
  from uk_pre_customer_spine_offer_is.td_offer_priceable_unit_p1 p1;
