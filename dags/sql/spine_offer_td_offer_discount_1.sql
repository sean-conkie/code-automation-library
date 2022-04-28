--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_td_offer_discount_1.sql
-- Author        : Nivedita Sanil/ Craig McKeever
-- Date Created  : 06 Sept 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Truncate and repopulate the TD table's that will populate the
--                 Dimension table to holds a history of status for order lines
--                 Table provides a history of the monetary value an offer would 
--                 discount. This will contain the first section
--
-- Comments      : NA
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : spine_offer_dim_1.sh
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
-- ------   ------  --------- --------------------------------------------  ----
-- 060921   nsa19             Initial version                               1.0
-- 080921   cmc37             Peer Programming                              1.1
-- 071121   nsa19   BDP-24837 Recalculate -ve discount value                1.3
--------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
-- The below look up fetches currency code from bsbpriceableunit and bsbbillingaccount tables. 
-- This table will be looked up by the code at the later stage to fetch currency code.
----------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_currency_lkp_1:WRITE_TRUNCATE:
select distinct portfolioofferid,
       ba.currencycode as currencycode
  from ( select portfolioofferid,
                id,
                billingaccountid,
                row_number() over (partition by id 
                                       order by effectivedate, 
                                                 created, 
                                                 id) rn
           from uk_tds_chordiant_eod_fc_is.fc_chordiant_bsbpriceableunit pu
          where pu.logically_deleted = 0
            and portfolioofferid is not null 
            and billingaccountid is not null 
        ) earliest_ba
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount ba
    on earliest_ba.billingaccountid = ba.id
 where rn = 1
   and ba.logically_deleted = 0 ;
   
----------------------------------------------------------------------------------------------------
-- This table will be looked up by the code at the later stage to fetch currency code. Service Instances 
-- have a hierarchical relationship with a billing service instance (type 300) forming the umbrella 
-- under which other services are contained.  This billing service instance has the relationship to 
-- the billing account
----------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_currency_lkp_2:WRITE_TRUNCATE:
select ch.id                                      as ch_id,
       pa.id                                      as pa_id,
       gp.id                                      as gp_id,
       ch.parentserviceinstanceid                 as ch_parent_service_instance_id,
       pa.parentserviceinstanceid                 as pa_parent_service_instance_id,
       case 
         when ch.serviceinstancetype = 300 then
           ch.id
         when pa.serviceinstancetype = 300 then
           pa.id
         when gp.serviceinstancetype = 300 then
           gp.id
         else
           gp.parentserviceinstanceid   
       end                                        as gp_parentserviceinstanceid
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbserviceinstance ch
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_bsbserviceinstance pa
    on pa.id = ch.parentserviceinstanceid
   and pa.logically_deleted  = 0
  left outer join  uk_tds_chordiant_eod_is.cc_chordiant_bsbserviceinstance gp
    on gp.id = pa.parentserviceinstanceid
   and gp.logically_deleted = 0 
 where ch.logically_deleted = 0 ;

----------------------------------------------------------------------------------------------
-- Extract all data and audit column from the offer source table. Populate the 
-- earliest entry point of an offer_id (portfolio_offer_id) into the audit columns. 
-- Calcualte lags to identify if there has been a change status between consecutive records.
----------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p1:WRITE_TRUNCATE:
select p_offer.id                                                          as portfolio_offer_id,
       p_offer.effective_from_dt                                           as source_effective_from_dt,
       p_offer.effective_from_dt_csn_seq                                   as effective_from_dt_csn_seq,
       p_offer.effective_from_dt_seq                                       as effective_from_dt_seq,
       ifnull(p_offer.statuschangeddate,effective_from_dt)                 as offer_statuschangeddate,
       lag(p_offer.statuschangeddate,1) over (partition by p_offer.id 
                                                  order by ifnull(p_offer.statuschangeddate,effective_from_dt),
                                                                  effective_from_dt,
                                                                  effective_from_dt_csn_seq,
                                                                  effective_from_dt_seq)  as prev_offer_statuschangeddate,                              
       pu.created                                                          as created_dt,
       pu.createdby                                                        as created_by_id,
       pu.lastupdate                                                       as last_modified_dt,
       pu.updatedby                                                        as last_modified_by_id,
       pu.id                                                               as portfolio_offer_priceable_unit_id,
       pu.discountedpriceableunitid                                        as discounted_priceable_unit_id,
       substr(pu2.priceableunitreferenceid,1,5)                            as discounted_product_id,
       p_offer.status                                                      as status,
       lag(p_offer.status,1) over (partition by p_offer.id 
                                       order by ifnull(p_offer.statuschangeddate,effective_from_dt),
                                                effective_from_dt,
                                                effective_from_dt_csn_seq,
                                                effective_from_dt_seq)     as previous_status,
       case
         when p_offer.status in ('ACT', 'BLK', 'PTM') then 
           true
         else
           false
       end                                                                  as active_flag,
       origportfolioofferid                                                 as original_portfolio_offer_id,
       p_offer.offerid,                                                     
       pu2.quotedprice                                                      as discounted_quoted_price,
       p_offer.serviceinstanceid                                            as serviceinstanceid
  from uk_tds_chordiant_eod_fc_is.fc_chordiant_bsbportfoliooffer p_offer
 inner join ( select portfolioofferid,
                     created,
                     createdby,
                     lastupdate,
                     updatedby,
                     id,
                     discountedpriceableunitid,
                     priceableunitreferenceid,
                     row_number() over (partition by portfolioofferid 
                                            order by created,
                                                     effectivedate,
                                                     effective_from_dt,
                                                     id,
                                                     effective_from_dt_csn_seq,
                                                     effective_from_dt_seq) as rn
                from uk_tds_chordiant_eod_fc_is.fc_chordiant_bsbpriceableunit
               where logically_deleted = 0 
              ) pu
     on p_offer.id = pu.portfolioofferid
  inner join uk_tds_chordiant_eod_is.cc_chordiant_bsbpriceableunit pu2
     on pu.discountedpriceableunitid = pu2.id
    and pu2.logically_deleted = 0  
  where rn = 1
    and p_offer.logically_deleted = 0
    and pu.discountedpriceableunitid is not null
    and pu2.priceableunitreferenceid is not null;
    
              
-----------------------------------------------------------------------------------------------
-- Drop records with no changes in status between consecutive records. We also check 
-- if statuschangeddate is the same despite change in statuses between consecutive records,
-- we populate the effective_from_dt from source table so that a change is captured.
-----------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p2:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.source_effective_from_dt,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       case 
         when td.offer_statuschangeddate = td.prev_offer_statuschangeddate 
          and td.status != ifnull(td.previous_status,'?') then
           source_effective_from_dt
         else offer_statuschangeddate
       end as offer_statuschangeddate,  
       td.prev_offer_statuschangeddate,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.status,
       td.previous_status,
       td.active_flag,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid
  from uk_pre_customer_spine_offer_is.td_offer_discount_p1 td
 where status != ifnull(previous_status,'?');
 
-------------------------------------------------------------------------------------------------------
--Calculate the next  offer_statuschangeddate
-------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p2_1:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.source_effective_from_dt,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.offer_statuschangeddate,  
       td.prev_offer_statuschangeddate,
       lead(td.offer_statuschangeddate,1) over (partition by td.portfolio_offer_id 
                                                  order by ifnull(td.offer_statuschangeddate,source_effective_from_dt),
                                                                  source_effective_from_dt,
                                                                  effective_from_dt_csn_seq,
                                                                  effective_from_dt_seq)  as next_offer_statuschangeddate,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.status,
       td.previous_status,
       td.active_flag,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid
  from uk_pre_customer_spine_offer_is.td_offer_discount_p2 td;
 
 
---------------------------------------------------------------------------------------------------
-- Recalculate offerstatuschangedate when it is same for two consecutive records and the status has 
-- Changed
--------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p2_2:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.source_effective_from_dt,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       case 
         when td.offer_statuschangeddate = td.next_offer_statuschangeddate 
          and td.status != ifnull(td.previous_status,'?') then
           source_effective_from_dt
         else offer_statuschangeddate
       end as offer_statuschangeddate,  
       td.prev_offer_statuschangeddate,
       td.next_offer_statuschangeddate,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.status,
       td.previous_status,
       td.active_flag,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid
  from uk_pre_customer_spine_offer_is.td_offer_discount_p2_1 td;
 
---------------------------------------------------------------------------------------------------------
-- Calculate activation_date and termination_date. Activation date is the earliest point is time
-- when the offer was activated. Temnation date is the point in time when the offer was in the state TMD
-- We will eliminate any offer_id's in source that are exisitng prior to their activation date. Also, we 
-- will eliminate offer id's post the termination date. Records are being eliminated as they don't 
-- add any value to the final offer stack calculation.
-- Activation_date and termination_date are critical is calculating effective_from_dt in next steps.
-- Also , notice this is the last point where active_flag is correctly calculated before it is recalculated
--in later steps.
-----------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p3:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.offer_statuschangeddate,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       ifnull(act.activation_date,termination_date)                      as activation_date,
       ifnull(tmd.termination_date,timestamp('2999-12-31 23:59:59 UTC')) as termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.status,
       td.previous_status,
       td.active_flag,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid
  from uk_pre_customer_spine_offer_is.td_offer_discount_p2_2 td
  left outer join ( select portfolio_offer_id,
                           ifnull(min(offer_statuschangeddate),timestamp('1901-01-01 00:00:00')) as activation_date
                      from uk_pre_customer_spine_offer_is.td_offer_discount_p2_2
                     where active_flag is true
                     group by portfolio_offer_id ) act
    on td.portfolio_offer_id = act.portfolio_offer_id
  left outer join ( select portfolio_offer_id,
                           ifnull(max(offer_statuschangeddate),timestamp('2999-12-31 23:59:59')) as termination_date
                      from uk_pre_customer_spine_offer_is.td_offer_discount_p2_2
                     where status = 'TMD'
                     group by portfolio_offer_id ) tmd
    on td.portfolio_offer_id = tmd.portfolio_offer_id
 where ifnull(act.activation_date,termination_date) <= offer_statuschangeddate 
   and offer_statuschangeddate <= ifnull(tmd.termination_date,timestamp('2999-12-31 23:59:59 UTC'));
     
---------------------------------------------------------------------------------------------------------------------
-- Calculate the effective_from_date in the next few steps. It will be a distinct set of dates that define the driver 
-- records to track any change that has happened accross multiple sources. Changes to an offer can come from 3 
-- different sources. bsbportfoliooffer drives changes in status, bsbcatalogueproductprice change in price and 
-- bsboffervaluediscount drives a change in discount.
-- Join to orderline and bsborder table to fetch the offerdiscountid related to the portfolio_offer_id to be calculated
--in next steps.
--------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p4_1:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.offer_statuschangeddate as effective_from_dt,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid,
       foo.discount_id,
       foo.currency     
  from uk_pre_customer_spine_offer_is.td_offer_discount_p3 td
  left outer join (select id,
                          portfolioofferid,
                          offerdiscountid as discount_id,
                          currency
                     from
                         ( select ol.id,
                                  ol.portfolioofferid,
                                  ol.offerdiscountid,
                                  ordr.currency,
                                  row_number () over (partition by ol.portfolioofferid 
                                                          order by ol.created, 
                                                                   ol.id) as rn
                             from uk_tds_chordiant_eod_is.cc_chordiant_bsborderline ol
                            inner join uk_tds_chordiant_eod_is.cc_chordiant_bsborder ordr
                               on ol.orderid  = ordr.id
                            where ol.logically_deleted   = 0
                              and ordr.logically_deleted = 0 
                              and ol.action   = 'AD'
                              and ordr.status !=  'APPCAN'
                         ) orders
                    where rn = 1
                   ) foo
    on td.portfolio_offer_id = foo.portfolioofferid
 where activation_date <= offer_statuschangeddate ;
   
   
----------------------------------------------------------------------------------------------
-- Create third look up to calculate currency code if it is still null. This lookup populates
-- a given combination of discounted_priceable_unit_id and effective_from_dt with the most
-- common currency under it
----------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_currency_lkp_3:WRITE_TRUNCATE:
select discounted_priceable_unit_id,
       effective_from_dt,
       currency ,
       currency_frequency ,
       most_common_currency 
  from
      (select discounted_priceable_unit_id,
              effective_from_dt,
              currency ,
              currency_frequency,
              row_number() over (partition by discounted_priceable_unit_id, effective_from_dt
                                     order by currency_frequency desc ) as most_common_currency 
         from
             (
               select discounted_priceable_unit_id,
                      effective_from_dt,
                      currency ,
                      count(1) over (partition by discounted_priceable_unit_id,effective_from_dt,currency) as currency_frequency
                 from uk_pre_customer_spine_offer_is.td_offer_discount_p4_1
                where currency is not null
             )
       )
 where most_common_currency = 1;
   
--------------------------------------------------------------------------------------------
-- If currency is null , use the look ups created at the begining to fetch a value
--------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p4:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.effective_from_dt,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid,
       td.discount_id,
       coalesce(td.currency,lkp1.currencycode,lkp2.currencycode,lkp3.currency) as  currency  
  from uk_pre_customer_spine_offer_is.td_offer_discount_p4_1 td 
  left outer join uk_pre_customer_spine_offer_is.td_offer_discount_currency_lkp_1 lkp1
    on td.portfolio_offer_id = lkp1.portfolioofferid
  left outer join ( select distinct si.gp_parentserviceinstanceid as billing_service_instance_id,
                           ba.currencycode               as currencycode
                      from uk_pre_customer_spine_offer_is.td_offer_discount_currency_lkp_2 si
                     inner join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount ba
                        on ba.serviceinstanceid = si.gp_parentserviceinstanceid
                     where ba.logically_deleted = 0 
                   ) lkp2
    on lkp2.billing_service_instance_id =  td.serviceinstanceid
  left outer join uk_pre_customer_spine_offer_is.td_offer_discount_currency_lkp_3 lkp3
    on td.discounted_priceable_unit_id = lkp3.discounted_priceable_unit_id
   and td.effective_from_dt            = lkp3.effective_from_dt;
       
--------------------------------------------------------------------------------------------
--Track all startdate where the prices have changed for the catalogueproductid related to 
-- discounted_priceable_unit_id while the offer is active
--------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p4:WRITE_APPEND:
select td.portfolio_offer_id,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       price.startdate as effective_from_dt,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid,
       td.discount_id,
       td.currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_p4 td
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsbcatalogueproductprice price
    on price.catalogueproductid = td.discounted_product_id
   and price.currencycode = td.currency
   and price.startdate >= td.activation_date
   and price.startdate <  td.termination_date
   and ifnull(price.rdmdeletedflag,'N') = 'N'
   and price.rdmaction != 'D';
                          
------------------------------------------------------------------------------------------------------------
-- Join to bsboffervaluediscount for the offerdiscountid related to the portfolio_offer_id where 
-- the bsboffervaluediscount.effectivefromdate occurred while the offer is active (i.e. active_flag is true)
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p4:WRITE_APPEND:   
select td.portfolio_offer_id,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       discount.effectivefromdate  as effective_from_dt,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.serviceinstanceid,
       td.discount_id,
       td.currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_p4 td   
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsboffervaluediscount discount
    on discount.offerdiscountid  = td.discount_id
   and discount.currencycode = td.currency
   and discount.effectivefromdate  >= td.activation_date
   and discount.effectivefromdate  <  td.termination_date
   and ifnull(discount.rdmdeletedflag,'N') = 'N'
   and discount.rdmaction != 'D';
 
-------------------------------------------------------------------------
-- Lag and check whether a change has occured
------------------------------------------------------------------------ 
uk_pre_customer_spine_offer_is.td_offer_discount_p5:WRITE_TRUNCATE:     
select portfolio_offer_id,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       effective_from_dt,
       lag(effective_from_dt,1) over (partition by portfolio_offer_id 
                                          order by effective_from_dt) as prev_effective_from_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       original_portfolio_offer_id,
       offerid,
       discounted_quoted_price,
       discount_id,
       currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_p4;

   
--------------------------------------------------------------------------
-- Elminate dates where no change has occured
--------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p6:WRITE_TRUNCATE:     
select portfolio_offer_id,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       effective_from_dt,
       prev_effective_from_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       original_portfolio_offer_id,
       offerid,
       discounted_quoted_price,
       discount_id,
       currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_p5
 where effective_from_dt != ifnull(prev_effective_from_dt,timestamp('1900-01-01'));

------------------------------------------------------------------------------------------------------------
-- 2nd point where driver records are defined. After effective_from_dt for an offer is calculated, find out 
-- distinct effective_from_dt for that priceable_unit_id. From there , generate new records !
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p7:WRITE_TRUNCATE:     
select td.portfolio_offer_id,
       lkp.effective_from_dt,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       activation_date,
       termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       max(td.original_portfolio_offer_id) as original_portfolio_offer_id ,
       max(td.offerid)                     as offerid,
       td.discounted_quoted_price,
       td.discount_id,
       max(currency)                       as currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_p6 td
  left outer join ( select distinct effective_from_dt,
                           discounted_priceable_unit_id
                      from uk_pre_customer_spine_offer_is.td_offer_discount_p6
                   ) lkp
    on lkp.discounted_priceable_unit_id = td.discounted_priceable_unit_id
 group by td.portfolio_offer_id,
          lkp.effective_from_dt,
          td.created_dt,
          td.created_by_id,
          td.last_modified_dt,
          td.last_modified_by_id,
          td.activation_date,
          td.termination_date,
          td.portfolio_offer_priceable_unit_id,
          td.discounted_priceable_unit_id,
          td.discounted_product_id,
          td.discounted_quoted_price,
          td.discount_id;
          
 
  