--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_td_offer_soip.sql
-- Author        : Emanuel Blei
-- Date Created  : 06th Aug 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate TD_FACT_OFFER_SOIP
--
-- Comments      : 2nd phase version.
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
-- 050821   ebi08    BQ       Initial Version for GCP Big Query             1.0
-- 271021   ebi08    BQ       Remove one version of cc_xds_discounts        1.1
--                            as source table, pac field populated from 
--                            customer_fulfilment_order_event.
-- 190122   ary17    BQ       Change in logic for order_line_id column      1.2
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Join data from driving tables
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_soip_p1:WRITE_TRUNCATE:
select offer.id                    id,
       offer.created               created_dt,
       offer.createdby             created_by_id,
       offer.lastupdate            last_modified_dt,
       offer.updatedby             last_modified_by_id,
       ba.portfolioid              portfolio_id,
       ba.id                       billing_account_id,
       ba.accountnumber            account_number,
       ser.id                      service_instance_id,
       ser.servicetype             ser_servicetype,
       ba.currencycode             currency_code,
	   offer.id                    first_portfolio_offer_id,
       dpi.id                      discount_id,
       offer.rcdiscountstartdate   offer_rcdiscountstartdate,
       offer.rcdiscountenddate     offer_rcdiscountenddate,
       offer.suid                  offer_detail_id,
       dpi.discounttypecode        transaction_type,
       dpi.rateid                  dpi_rateid,
	   dpi.id                      dpi_id
  from uk_tds_chordiant_eod_is.cc_chordiant_offer                         offer
 inner join uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitem      dpi
    on (    offer.id = dpi.offerid
        and dpi.logically_deleted = 0)
 inner join uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitemlink  dpil
    on (   dpi.id = dpil.discountpricingitem
        and dpil.logically_deleted = 0)
 inner join uk_tds_chordiant_eod_is.cc_chordiant_pricingitem              pri
    on (    dpil.pricingitemid = pri.id
        and pri.logically_deleted = 0)
 inner join uk_tds_chordiant_eod_is.cc_chordiant_productpricingitemlink   ppil
    on (    pri.id = ppil.pricingitemid
        and ppil.logically_deleted = 0)
 inner join uk_tds_chordiant_eod_is.cc_chordiant_product                  pro
    on (    ppil.productid = pro.id
        and pro.logically_deleted = 0)
 inner join uk_tds_chordiant_eod_is.cc_chordiant_service                  ser
    on (    pro.serviceid = ser.id
        and ser.logically_deleted = 0)
 inner join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount        ba
    on (    ser.billingserviceinstanceid = ba.serviceinstanceid
        and ba.logically_deleted = 0)
 where offer.logically_deleted = 0;


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Join driving tables to non-driving tables.
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_soip_p2:WRITE_TRUNCATE:
select p1.id                          id,
       p1.created_dt                  created_dt,
       p1.created_by_id               created_by_id,
       p1.last_modified_dt            last_modified_dt,
       p1.last_modified_by_id         last_modified_by_id,
       p1.portfolio_id                portfolio_id,
       p1.billing_account_id          billing_account_id,
       p1.account_number              account_number,
       p1.service_instance_id         service_instance_id,
       p1.ser_servicetype             ser_servicetype,
       p1.currency_code               currency_code,
       oe.order_id                    order_id,
       to_base64(sha512(concat(oe.order_id,':',
                               oe.offer_id,':',
                               dsprit.pricingitemid)
                       )
                )                     order_line_id,
       p1.id                          first_portfolio_offer_id,
       p1.discount_id                 discount_id,
       dis.discount_basis             discount_type,
       com.duration                   com_duration,
       dp.value                       discount,
       p1.offer_rcdiscountstartdate   offer_rcdiscountstartdate,
       p1.offer_rcdiscountenddate     offer_rcdiscountenddate,
       p1.offer_detail_id             offer_detail_id,
       com.offer_name                 description,
       dis.agent_facing_name          name,
       dis.customer_facing_name       bill_name,
       p1.transaction_type            transaction_type,
       oe.pac                         pac,
       dos.status_code                dos_status_code
  from uk_pre_customer_spine_offer_is.td_fact_offer_soip_p1               p1
  left outer join (
         select orderid                                order_id,
                offers_struct.id                       offer_id,
                offers_struct.pac                      pac
           from uk_tds_customer_fulfilment_eod_is.cc_customer_fulfilment_order_event,
         unnest (offers)                               offers_struct
          where offers_struct.action = 'ADD'
                  )                                                       oe
    on p1.id = oe.offer_id
  inner join ( 
         select discountpricingitem,
                pricingitemid,
                created,
                row_number() over (partition by discountpricingitem 
                                            order by created
                                                     ) as rn
                from uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitemlink
               where logically_deleted = 0 
              ) dsprit
     on (p1.dpi_id = dsprit.discountpricingitem
     and rn = 1)
  left outer join uk_pub_customer_spine_offer_is.dim_offer_status         dos
    on (    p1.id = dos.portfolio_offer_id
        and dos.effective_to_dt = '2999-12-31 23:59:59')
  left outer join uk_tds_xds_eod_is.cc_xds_commercial_offers              com
    on p1.offer_detail_id = com.suid
  left outer join uk_tds_xds_eod_is.cc_xds_discounts                      dis
    on p1.dpi_rateid = dis.rate_id
  left outer join uk_tds_xds_eod_is.cc_xds_discounts_pricing              dp
    on (    dis.suid = dp.suid
        and p1.currency_code = dp.currency);


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Carry out transformation on data from source tables
-- Add default values for columns without source input
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_soip:WRITE_TRUNCATE:
select id                                        id,
       created_dt                                created_dt,
       created_by_id                             created_by_id,
       last_modified_dt                          last_modified_dt,
       last_modified_by_id                       last_modified_by_id,
       portfolio_id                              portfolio_id,
       billing_account_id                        billing_account_id,
       account_number                            account_number,
       service_instance_id                       service_instance_id,
       case
         when ser_servicetype = 'SOIP' then      
           'SOIP'                                
         when ser_servicetype = 'AMP'  then      
           'APPS MARKETPLACE'                    
         else                                    
           'OTHER'                               
       end                                       account_type,
       currency_code                             currency_code,
       order_id                                  order_id,
       order_line_id                             order_line_id,
       cast(null as string)                      previous_portfolio_offer_id,
       first_portfolio_offer_id                  first_portfolio_offer_id,
       discount_id                               discount_id,
       discount_type                             discount_type,
       cast(regexp_replace(com_duration,
                           '[PM]', '') as int64) discount_duration,
       discount                                  discount,
       ifnull(offer_rcdiscountstartdate,
              timestamp('1900-01-01'))           start_dt,
       ifnull(offer_rcdiscountenddate,
              timestamp('2999-12-31 23:59:59'))  end_dt,
       cast(null as string)                      order_urn,
       offer_detail_id                           offer_detail_id,
       cast(null as string)                      code,
       description                               description,
       name                                      name,
       bill_name                                 bill_name,
       transaction_type                          transaction_type,
       pac                                       pac,
       cast(null as string)                      version,
       case 
         when dos_status_code = 'ACT' then
           true
         else
           false
       end                                       active_flag,
       cast(null as boolean)                     automatic_flag,
       false                                     gross_flag,
       false                                     global_flag,
       cast(null as boolean)                     price_freeze_offer_flag,
       cast(null as boolean)                     in_contract_offer_flag
  from uk_pre_customer_spine_offer_is.td_fact_offer_soip_p2;
