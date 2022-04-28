--------------------------------------------------------------------------------
--
-- Filename      : spine_td_offer_priceable_unit_soip.sql
-- Author        : Ankita Gaikar
-- Date Created  : 3rd August 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate TD_OFFER_PRICEABLE_UNIT_SOIP as a precursor to 
--                 populating DIM_OFFER_PRICEABLE_UNIT
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
-- 191121   asv20   BQ        granularity fix after xds_issue               1.3
--------------------------------------------------------------------------------
-- =============================================================================
-- Getting SOIP Data
-- Get sub-set of data from driving table cc_chordiant_discountpricingitemlink
-- Join to 13 other tables for data enrichment
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_priceable_unit_soip_p1:WRITE_TRUNCATE:
select to_base64(sha512(concat(dpl.discountpricingitem,
                                ':',
                                dpl.pricingitemid)))  id,
       dpl.created                                    created_dt,
       dpl.createdby                                  created_by_id,
       dpl.lastupdate                                 last_modified_dt,
       dpl.updatedby                                  last_modified_by_id,
       dp.offerid                                     portfolio_offer_id,
       dpl.pricingitemid                              discounted_priceable_unit_id,
       dp.effectivefromdate                           effective_dt,
       dp.effectivetodate                             end_dt,
       case 
         when dis.discount_basis = 'VALUE' then
           dip.value / 100
         when dis.discount_basis = 'PERCENTAGE' then
           round(safe_multiply(rp.value/100,
                               cast(dip.percentage as numeric)/100),
                 2)
         else 
           cast(null as numeric)
       end                                            quoted_discount
  from uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitemlink           dpl
  inner join uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitem         dp
    on (    dpl.discountpricingitem = dp.id
        and dp.logically_deleted = 0)
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_pricingitem            pr
    on (    dpl.pricingitemid = pr.id
        and pr.logically_deleted = 0)
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_productpricingitemlink ppl
    on (    pr.id = ppl.pricingitemid
        and ppl.logically_deleted = 0)
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_product                pro
    on (    ppl.productid = pro.id
        and pro.logically_deleted = 0)
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_service                ser
    on (    pro.serviceid = ser.id
        and ser.logically_deleted = 0)
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount      ba
    on (    ser.billingserviceinstanceid = ba.serviceinstanceid
        and ba.logically_deleted = 0)
  inner join (select id,
                  catalogueversion, rn
          from (select pro.id,
                       oe.catalogueversion,
                       row_number() over (partition by pro.id
                                              order by oe.ordercreateddatetime) rn
                  from uk_tds_customer_fulfilment_eod_is.cc_customer_fulfilment_order_event oe,
                       unnest(products) pro
                  where pro.action = 'ADD')
         where rn = 1) cfoe
    on ppl.productid = cfoe.id
  left join uk_tds_xds_eod_is.cc_xds_rates                                    rat
    on (    pr.rateid = rat.rate_id
        and rat.catalogue_version = cfoe.catalogueversion
       )
  left join uk_tds_xds_eod_is.cc_xds_rates_pricing                            rp
    on (    rat.suid = rp.suid
            and ba.currencycode = rp.currency
            and rp.catalogue_version=cfoe.catalogueversion)
  left join uk_tds_chordiant_eod_is.cc_chordiant_offer                        ofr
    on (    dp.offerid = ofr.id
        and ofr.logically_deleted = 0)
  left join uk_tds_xds_eod_is.cc_xds_commercial_offers                        co
    on ofr.suid = co.suid
  left join (select suid,
                    discount_name,
                    row_number() over (partition by suid
                                           order by cast(replace(publish_model_version, '.', '') as int) desc) rn
               from uk_tds_xds_eod_is.cc_xds_commercial_offers_discounts) cod
    on co.suid = cod.suid
   and cod.rn  = 1
  left join uk_tds_xds_eod_is.cc_xds_discounts                                dis
    on cod.discount_name = dis.suid
  left join uk_tds_xds_eod_is.cc_xds_discounts_pricing                        dip
    on dis.suid = dip.suid
 where dpl.logically_deleted = 0;

----------------------------------------------------------------------------------------------------------
-- Write_truncate Insert data from SOIP to td table
----------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_priceable_unit_soip:WRITE_TRUNCATE:
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
  from uk_pre_customer_spine_offer_is.td_offer_priceable_unit_soip_p1 p1;
