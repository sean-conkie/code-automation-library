--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_td_offer_discount_soip.sql
-- Author        : Aditya Choudhury / Nivedita Sanil
-- Date Created  : 24 Dec 2021
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
-- 241221   acr73             Initial version                               1.0
-- 271221   nsa19             Peer Programming                              1.1
-- 090222   acr73   BDCF-447  replacing left joins with inner joins 
--                            for required fields tables                    1.2
-- 180222   acr73             null check for dicountstartdate defaulting  
--                            to high date in status_code cal. Modification
--                            to offer stacking logic as like core          1.3
-- 080322   acr73             0 discounted_product_price scenario           1.4
--------------------------------------------------------------------------------
---------------------------------------------------------------------------------
-- Price look up with audit cols from discountpricingitem
---------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1:WRITE_TRUNCATE:
select offer.id                                       portfolio_offer_id,
       dpil.created                                   effective_from_dt,
       offer.effective_from_dt_csn_seq,
       offer.effective_from_dt_seq,
       offer.suid,
       offer.created,
       offer.createdby,
       offer.lastupdate,
       offer.updatedby,
       offer.rcdiscountstartdate,
       offer.rcdiscountenddate,
       offer.eventcode,
       offer.ispendingcancel, 
       bba.currencycode                               currency,
       to_base64(
          sha512(
          concat(dpil.discountpricingitem, ':',
                 dpil.pricingitemid)))                portfolio_offer_priceable_unit_id,
       dpil.pricingitemid                             discounted_priceable_unit_id,
       prod.suid                                      discounted_product_id,
       cast(coalesce(rp.price /100 ,0) as numeric)    discounted_product_price,
       dpi.discounttypecode                           transaction_type,
       row_number() over (partition by offer.id
                              order by dpil.created)  rnum
  from uk_tds_chordiant_eod_fc_is.fc_chordiant_offer offer
  left join uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitem dpi
    on offer.id              = dpi.offerid
   and dpi.logically_deleted = 0
 inner join uk_tds_chordiant_eod_is.cc_chordiant_discountpricingitemlink dpil 
    on dpi.id                 = dpil.discountpricingitem 
   and dpil.logically_deleted = 0
  left join uk_tds_chordiant_eod_is.cc_chordiant_productpricingitemlink ppil
    on dpil.pricingitemid     = ppil.pricingitemid
   and ppil.logically_deleted = 0
 inner join uk_tds_chordiant_eod_is.cc_chordiant_pricingitem pi
    on pi.id                = ppil.pricingitemid
   and pi.logically_deleted = 0
 inner join uk_tds_chordiant_eod_is.cc_chordiant_product prod
    on ppil.productid         = prod.id
   and prod.logically_deleted = 0 
  left join uk_tds_chordiant_eod_is.cc_chordiant_service svc
    on prod.serviceid        = svc.id
   and svc.logically_deleted = 0
  left join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount bba 
    on svc.billingserviceinstanceid = bba.serviceinstanceid 
   and bba.logically_deleted        = 0
  left join ( select foe.catalogueVersion, 
                     p.id, 
                     p.action
                from uk_tds_customer_fulfilment_eod_is.cc_customer_fulfilment_order_event foe,
                     unnest(products) p ) oe
    on prod.id             = oe.id 
   and upper(oe.action)    = 'ADD'
  left join uk_tds_xds_eod_is.cc_xds_rates rates
    on pi.rateid           = rates.rate_id 
   and oe.catalogueVersion = rates.catalogue_version
  left join uk_tds_xds_eod_is.cc_xds_rates_pricing rp
    on rates.suid          = rp.suid 
   and bba.currencycode    = rp.currency 
   and oe.catalogueVersion = rp.catalogue_version
 where offer.logically_deleted = 0;

-----------------------------------------------------------------------------------------
-- Lookup to calculate the price_effective_to_dt
-----------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1_lkp:WRITE_TRUNCATE:
select distinct portfolio_offer_id,
       effective_from_dt,
       lead(effective_from_dt
           ,1) over (partition by portfolio_offer_id
                         order by effective_from_dt,
                         effective_from_dt_csn_seq,
                         effective_from_dt_seq) price_effective_to_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq, 
       portfolio_offer_priceable_unit_id,	
       discounted_priceable_unit_id
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1
 where rcdiscountstartdate is not null 
   and rcdiscountenddate is not null; 

----------------------------------------------------------------------------------------
-- price_end_date calculation
----------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1_1:WRITE_TRUNCATE:
select p1.portfolio_offer_id,
       p1.effective_from_dt,
       lkp.price_effective_to_dt,
       p1.effective_from_dt_csn_seq,
       p1.effective_from_dt_seq,
       p1.suid,
       p1.created,
       p1.createdby,
       p1.lastupdate,
       p1.updatedby,
       p1.rcdiscountstartdate,
       p1.rcdiscountenddate,
       p1.eventcode,
       p1.ispendingcancel, 
       p1.currency,
       p1.portfolio_offer_priceable_unit_id,
       p1.discounted_priceable_unit_id,
       p1.discounted_product_id,
       p1.discounted_product_price,
       p1.transaction_type,
       p1.rnum
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1 p1
  left join uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1_lkp lkp
    on p1.portfolio_offer_id = lkp.portfolio_offer_id
   and p1.effective_from_dt = lkp.effective_from_dt
   and p1.effective_from_dt_csn_seq = lkp.effective_from_dt_csn_seq
   and p1.effective_from_dt_seq = lkp.effective_from_dt_seq
   and p1.portfolio_offer_priceable_unit_id = lkp.portfolio_offer_priceable_unit_id
   and p1.discounted_priceable_unit_id = lkp.discounted_priceable_unit_id;


----------------------------------------------------------------------------------
-- UNION FC records with status drivers with Price lookup
-- this is done to bring in alll the other records which could fall into the status cal.
----------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p2:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       price_effective_to_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created,
       createdby,
       lastupdate,
       updatedby,
       rcdiscountstartdate,
       rcdiscountenddate,
       eventcode,
       ispendingcancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1_1
 union all 
select offer.id,
       offer.effective_from_dt,
       ifnull(price.price_effective_to_dt,timestamp('2999-12-31 23:59:59 UTC')),
       offer.effective_from_dt_csn_seq,
       offer.effective_from_dt_seq,
       offer.suid,
       offer.created,
       offer.createdby,
       offer.lastupdate,
       offer.updatedby,
       offer.rcdiscountstartdate,
       offer.rcdiscountenddate,
       offer.eventcode,
       offer.ispendingcancel,
       price.portfolio_offer_priceable_unit_id,
       price.discounted_priceable_unit_id,
       price.discounted_product_id,
       price.discounted_product_price,
       price.transaction_type 
  from uk_tds_chordiant_eod_fc_is.fc_chordiant_offer offer
  left join uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1_1 price
    on offer.id = price.portfolio_offer_id
   and offer.effective_from_dt >= price.effective_from_dt
   and offer.effective_from_dt < ifnull(price.price_effective_to_dt,timestamp('2999-12-31 23:59:59 UTC'))
 where offer.logically_deleted = 0;

----------------------------------------------------------------------------------
--calculating previous ispendingcacel for furthur calculation
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p3:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt                                   source_effective_from_dt,
       price_effective_to_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created,
       createdby,
       lastupdate,
       updatedby,
       rcdiscountstartdate,
       rcdiscountenddate,
       eventcode,
       ispendingcancel,
       lag(ispendingcancel,
          1) over (partition by portfolio_offer_id 
                       order by effective_from_dt,
                                effective_from_dt_csn_seq,
                                effective_from_dt_seq)     previous_ispendingcancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p2;
-------------------------------------------------------------------------------------
-- Calculating effective_from_dt, status_code and state based on condition  
-- (rcdiscountstartdate is null or rcdiscountstartdate > source_effective_from_dt)
--  and created < rcdiscountstartdate
-------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p4:WRITE_TRUNCATE:
select portfolio_offer_id,
       source_effective_from_dt,
       created                                  effective_from_dt,
       'PND'                                    status_code,
       'PREACTIVE'                              state,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created                                  created_dt,
       createdby                                created_by_id,
       lastupdate                               last_modified_dt,
       updatedby                                last_modified_by_id,
       rcdiscountstartdate                      discount_startdate,
       rcdiscountenddate                        discount_enddate,
       eventcode                                event_code,
       ispendingcancel                          is_pending_cancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p3
 where (    rcdiscountstartdate is null 
        or rcdiscountstartdate > source_effective_from_dt)
   and created < ifnull(rcdiscountstartdate,timestamp('2999-12-31 23:59:59 UTC'));

-----------------------------------------------------------------------------------
-- Appending resultant from second condition
-----------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p4:WRITE_APPEND:
select portfolio_offer_id,
       source_effective_from_dt,
       case 
         when created < rcdiscountstartdate then 
           rcdiscountstartdate
         when date(source_effective_from_dt) = date(rcdiscountstartdate) then 
           source_effective_from_dt
         when date(created) = date(rcdiscountstartdate) then 
           created
         else 
           rcdiscountstartdate
       end                                      effective_from_dt,
       'ACT'                                    status_code,
       'ACTIVE'                                 state,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created                                  created_dt,
       createdby                                created_by_id,
       lastupdate                               last_modified_dt,
       updatedby                                last_modified_by_id,
       rcdiscountstartdate                      discount_startdate,
       rcdiscountenddate                        discount_enddate,
       eventcode                                event_code,
       ispendingcancel                          is_pending_cancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p3
 where rcdiscountstartdate is not null 
   and (   rcdiscountenddate is null 
        or rcdiscountenddate > source_effective_from_dt) 
   and (   ispendingcancel is null 
        or ispendingcancel = 0) 
   and (   previous_ispendingcancel is null 
        or previous_ispendingcancel = 0);

----------------------------------------------------------------------------
-- Appending resultant from third condition
----------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p4:WRITE_APPEND:
select portfolio_offer_id,
       source_effective_from_dt,
       source_effective_from_dt                 effective_from_dt,
       'PTM'                                    status_code,
       'ACTIVE'                                 state,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created                                  created_dt,
       createdby                                created_by_id,
       lastupdate                               last_modified_dt,
       updatedby                                last_modified_by_id,
       rcdiscountstartdate                      discount_startdate,
       rcdiscountenddate                        discount_enddate,
       eventcode                                event_code,
       ispendingcancel                          is_pending_cancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p3
 where rcdiscountstartdate is not null 
   and (   rcdiscountenddate is null 
        or rcdiscountenddate > source_effective_from_dt)
   and ispendingcancel = 1;

----------------------------------------------------------------------------
-- Appending resultant from fourth condition
----------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p4:WRITE_APPEND:
select portfolio_offer_id,
       source_effective_from_dt,
       source_effective_from_dt                 effective_from_dt,
       'ACT'                                    status_code,
       'ACTIVE'                                 state,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created                                  created_dt,
       createdby                                created_by_id,
       lastupdate                               last_modified_dt,
       updatedby                                last_modified_by_id,
       rcdiscountstartdate                      discount_startdate,
       rcdiscountenddate                        discount_enddate,
       eventcode                                event_code,
       ispendingcancel                          is_pending_cancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p3
 where rcdiscountstartdate is not null 
   and (   rcdiscountenddate is null 
        or rcdiscountenddate > source_effective_from_dt) 
   and (   ispendingcancel is null 
        or ispendingcancel = 0) 
   and previous_ispendingcancel = 1;


----------------------------------------------------------------------------
-- Appending resultant from fifth condition
----------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p4:WRITE_APPEND:
select portfolio_offer_id,
       source_effective_from_dt,
       greatest(source_effective_from_dt, rcdiscountenddate) effective_from_dt,
       'TMD'                                    status_code,
       'INACTIVE'                               state,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created                                  created_dt,
       createdby                                created_by_id,
       lastupdate                               last_modified_dt,
       updatedby                                last_modified_by_id,
       rcdiscountstartdate                      discount_startdate,
       rcdiscountenddate                        discount_enddate,
       eventcode                                event_code,
       ispendingcancel                          is_pending_cancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p3
 where rcdiscountstartdate is not null 
   and rcdiscountstartdate <= source_effective_from_dt 
   and rcdiscountenddate is not null 
   and rcdiscountenddate <= current_timestamp();
 
--------------------------------------------------------------------------------------
-- CDC on status code | calculating active flag and prev_status_code
--------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p5:WRITE_TRUNCATE:
select portfolio_offer_id,
       source_effective_from_dt,
       effective_from_dt,
       lag(effective_from_dt,
           1) over (partition by portfolio_offer_id 
                        order by effective_from_dt,
                                 source_effective_from_dt,
                                 effective_from_dt_csn_seq,
                                 effective_from_dt_seq) prev_effective_from_dt,
       status_code,
       lag(status_code,
           1) over (partition by portfolio_offer_id 
                        order by effective_from_dt,
                                 source_effective_from_dt,
                                 effective_from_dt_csn_seq,
                                 effective_from_dt_seq)  prev_status_code,
       state,
       case
         when state = 'ACTIVE' then 
           true
         else
           false
       end                                   active_flag,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       discount_startdate,
       discount_enddate,
       event_code,
       is_pending_cancel,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       lag(discounted_product_price,
           1) over (partition by portfolio_offer_id 
                        order by effective_from_dt,
                                 source_effective_from_dt,
                                 effective_from_dt_csn_seq,
                                 effective_from_dt_seq)  prev_discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p4;

--------------------------------------------------------------------------------------
-- Calculating activation and termination date and filtering anything outside this range
--------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p6:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.source_effective_from_dt,
       td.effective_from_dt,
       td.prev_effective_from_dt,
       td.status_code,
       td.prev_status_code,
       td.state,
       td.active_flag,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.suid,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.discount_startdate,
       td.discount_enddate,
       td.event_code,
       td.is_pending_cancel,
       ifnull(act.activation_date,termination_date)                      as activation_date,
       ifnull(tmd.termination_date,timestamp('2999-12-31 23:59:59 UTC')) as termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.discounted_product_price,
       td.prev_discounted_product_price,
       td.transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p5 td
  left outer join ( select portfolio_offer_id,
                           ifnull(min(discount_startdate),timestamp('1901-01-01 00:00:00')) as activation_date
                      from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p5
                     where active_flag is true
                     group by portfolio_offer_id ) act
    on td.portfolio_offer_id = act.portfolio_offer_id
  left outer join ( select portfolio_offer_id,
                           ifnull(max(discount_enddate),timestamp('2999-12-31 23:59:59')) as termination_date
                      from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p5
                     where state = 'TMD'
                     group by portfolio_offer_id ) tmd
    on td.portfolio_offer_id = tmd.portfolio_offer_id
 where ifnull(act.activation_date,termination_date) <= effective_from_dt 
   and effective_from_dt <= ifnull(tmd.termination_date,timestamp('2999-12-31 23:59:59 UTC')); 

--------------------------------------------------------------------------------------
-- Filtering on CDC of status, effective_from_dt and discounted_product_price
-------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p7:WRITE_TRUNCATE:
select portfolio_offer_id,
       source_effective_from_dt,
       effective_from_dt,
       prev_effective_from_dt,
       status_code,
       prev_status_code,
       state,
       active_flag,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       suid,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       discount_startdate,
       discount_enddate,
       event_code,
       is_pending_cancel,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_price,
       prev_discounted_product_price,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p6
 where ifnull(status_code,'?') != ifnull(prev_status_code,'?')
    or effective_from_dt != ifnull(prev_effective_from_dt,timestamp('1900-01-01'))
    or ifnull(discounted_product_price,0) != ifnull(prev_discounted_product_price,0);
  
--------------------------------------------------------------------------------------
-- Introducing discount columns from discount tables
--------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p8:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       td.source_effective_from_dt,
       case 
	       when td.effective_from_dt = td.prev_effective_from_dt
		     and (   ifnull(td.status_code, '?') != ifnull(td.prev_status_code, '?')
               or ifnull(td.discounted_product_price,0) != ifnull(td.prev_discounted_product_price,0)) then
	       source_effective_from_dt
		   else 
         td.effective_from_dt
	     end                                               effective_from_dt, 
       td.prev_effective_from_dt,
       td.prev_status_code,
       td.status_code,
       td.active_flag,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.suid,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       cast(null as string)                                original_portfolio_offer_id,
       td.discount_startdate,
       td.discount_enddate,
       td.event_code,
       td.is_pending_cancel,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.discounted_product_price,
       td.prev_discounted_product_price,
       td.transaction_type,
       dis.suid                                      discount_id,
       case 
         when contains_substr(dis.suid, 'PHO') then 
           'PRICE PROTECTION'
         else 
           dis.discount_basis
       end                                           discount_type,
       cast(dis.discount_priority as integer)        discount_priority,
       cast(null as integer)                         discount_version,
       case 
         when upper(dis.discount_basis) = 'VALUE' then 
           cast(dp.value /100 as numeric)
         when upper(dis.discount_basis) = 'PERCENTAGE' then 
           cast(dp.percentage as numeric)
       end                                           discount
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p7 td 
  left join uk_tds_xds_eod_is.cc_xds_commercial_offers_discounts cod
    on td.suid = cod.suid
  left join uk_tds_xds_eod_is.cc_xds_discounts dis
    on cod.discount_name = dis.suid
  left join uk_tds_xds_eod_is.cc_xds_discounts_pricing dp 
    on cod.discount_name = dp.suid; 

--------------------------------------------------------------------------------------
-- Calculating discounted_product_stating_price and currency
-----------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9:WRITE_TRUNCATE:
select td.portfolio_offer_id, 
       td.effective_from_dt,
       td.source_effective_from_dt,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.prev_status_code,
       td.status_code,
       td.active_flag,
       td.suid,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.original_portfolio_offer_id,
       td.discount_startdate,
       td.discount_enddate,
       td.event_code,
       td.is_pending_cancel,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       p1.discounted_product_price                               discounted_product_starting_price,
       td.discounted_product_price                               discounted_product_current_price,
       p1.currency,
       td.transaction_type,
       td.discount_id,
       td.discount_type,
       td.discount_priority,
       td.discount_version,
       td.discount,
       row_number() over ( partition by td.portfolio_offer_id, 
                                        td.effective_from_dt
                               order by td.portfolio_offer_id, 
                                        td.effective_from_dt, 
                                        td.source_effective_from_dt  desc) rn
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p8 td
  left join uk_pre_customer_spine_offer_is.td_offer_discount_soip_p1 p1
    on td.portfolio_offer_id = p1.portfolio_offer_id
   and p1.rnum = 1;    


------------------------------------------------------------------------------------------------------------
-- Filtering on row number as 1 to get the latest of the source_effective_from_dt row when 
-- there's multiple rows with same effective_from_dt
------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9_1:WRITE_TRUNCATE:
select portfolio_offer_id, 
       effective_from_dt,
       source_effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       prev_status_code,
       status_code,
       active_flag,
       suid,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       original_portfolio_offer_id,
       discount_startdate,
       discount_enddate,
       event_code,
       is_pending_cancel,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_starting_price,
       discounted_product_current_price,
       currency,
       transaction_type,
       discount_id,
       discount_type,
       discount_priority,
       discount_version,
       discount
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9
  where rn = 1;    

------------------------------------------------------------------------------------------------------------
-- 2nd point where driver records are defined. After effective_from_dt for an offer is calculated, find out 
-- distinct effective_from_dt for that priceable_unit_id. From there , generate new records !
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p10:WRITE_TRUNCATE:
select td.portfolio_offer_id,
       lkp.effective_from_dt,
       max(td.created_dt)                  as created_dt,
       max(td.created_by_id)               as created_by_id,
       max(td.last_modified_dt)            as last_modified_dt,
       max(td.last_modified_by_id)         as last_modified_by_id,
       activation_date,
       termination_date,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.discounted_product_starting_price,
       td.discount_id,
       td.discount_type,
       max(currency)                      as currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9_1 td
  left outer join ( select distinct effective_from_dt,
                           discounted_priceable_unit_id,
                           last_modified_dt
                      from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9_1
                   ) lkp
    on lkp.discounted_priceable_unit_id = td.discounted_priceable_unit_id
   and lkp.last_modified_dt = td.last_modified_dt
 group by td.portfolio_offer_id,
          lkp.effective_from_dt,
          td.activation_date,
          td.termination_date,
          td.portfolio_offer_priceable_unit_id,
          td.discounted_priceable_unit_id,
          td.discounted_product_id,
          td.discounted_product_starting_price,
          td.discount_id,
          td.discount_type;
          
------------------------------------------------------------------------------------------------------------
-- Recalculate active_flag for exisiting records from source and those for the generated records
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p11:WRITE_TRUNCATE:  
select td.portfolio_offer_id,
       td.effective_from_dt,
       ifnull(p.effective_from_dt_csn_seq,latest_flag.effective_from_dt_csn_seq) as effective_from_dt_csn_seq,
       ifnull(p.effective_from_dt_seq,latest_flag.effective_from_dt_seq)         as effective_from_dt_seq,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.activation_date,
       td.termination_date ,
       td.portfolio_offer_priceable_unit_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       case 
         when p.active_flag is not null then
           true
         else
           false
       end                                           as source_field,
       ifnull(p.active_flag,latest_flag.active_flag) as active_flag,
       ifnull(p.status_code,latest_flag.status)      as status_code,
       td.discounted_product_starting_price,
       cast('false' as boolean)                      gross_flag,
       cast(null as integer)                         offer_detail_id,
       p.transaction_type,
       p.discount_id,
       p.discount_type,
       p.discount_priority,
       td.currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p10 td
  left outer join uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9_1 p
    on td.portfolio_offer_id = p.portfolio_offer_id
   and td.effective_from_dt  = p.effective_from_dt
   and td.created_dt = p.created_dt
   and td.created_by_id = p.created_by_id
   and td.last_modified_dt = p.last_modified_dt
   and td.last_modified_by_id = p.last_modified_by_id
  left outer join ( select portfolio_offer_id,
                           effective_from_dt,
                           effective_from_dt_csn_seq,
                           effective_from_dt_seq,
                           status,
                           active_flag
                      from (select 
                                   p10.portfolio_offer_id,
                                   p10.effective_from_dt,
                                   p9.effective_from_dt_csn_seq,
                                   p9.effective_from_dt_seq,
                                   p9.activation_date,
                                   p9.status_code status,
                                   p9.active_flag,
                                   p9.termination_date,
                                   row_number() over (partition by p10.portfolio_offer_id, p10.effective_from_dt
                                                          order by p10.activation_date       desc,
                                                                   p10.effective_from_dt     desc,
                                                                   effective_from_dt_csn_seq desc,
                                                                   effective_from_dt_seq     desc) as rn
                             from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p10 p10 
                             left outer join uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9_1 p9
                               on p9.portfolio_offer_id = p10.portfolio_offer_id
                              and p9.created_dt = p10.created_dt
                              and p9.created_by_id = p10.created_by_id
                              and p9.last_modified_dt = p10.last_modified_dt
                              and p9.last_modified_by_id = p10.last_modified_by_id
                              and p10.effective_from_dt > p9.activation_date
                              and p10.effective_from_dt < p9.termination_date
                             ) foo
                     where rn =1
                   ) latest_flag
    on td.portfolio_offer_id = latest_flag.portfolio_offer_id
   and td.effective_from_dt  = latest_flag.effective_from_dt;

---------------------------------------------------------------------------------------
-- Default all null active_flags outside of activation and termination date as as false. Calculate price and
-- discount values.
------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p12:WRITE_TRUNCATE:    
select td.portfolio_offer_id,
       td.effective_from_dt,
       td.effective_from_dt_csn_seq,
       td.effective_from_dt_seq,
       td.created_dt,
       td.created_by_id,
       td.last_modified_dt,
       td.last_modified_by_id,
       td.activation_date,
       td.termination_date,
       td.portfolio_offer_priceable_unit_id,
       to_base64(sha512
                (concat(td.portfolio_offer_priceable_unit_id,':',
                   cast(td.effective_from_dt as string))))       discounted_product_event_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.source_field,
       case 
         when td.active_flag is null and (td.effective_from_dt < td.activation_date or td.effective_from_dt > td.termination_date ) then
           false
         else 
           td.active_flag
       end                                                      active_flag,
       td.status_code,
       td.discounted_product_starting_price,
       ifnull(price.discounted_product_current_price,0)         discounted_product_current_price,
       td.gross_flag,
       td.transaction_type,
       td.discount_id,
       td.discount_type,
       td.currency,
       td.discount_priority,
       price.discount_version,
       price.discount
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p11 td
  left outer join uk_pre_customer_spine_offer_is.td_offer_discount_soip_p9_1 price
    on td.portfolio_offer_id = price.portfolio_offer_id
   and td.effective_from_dt = price.effective_from_dt
   and td.created_dt = price.created_dt
   and td.created_by_id = price.created_by_id
   and td.last_modified_dt = price.last_modified_dt
   and td.last_modified_by_id = price.last_modified_by_id
   and td.effective_from_dt >= ifnull(price.discount_startdate,timestamp('1901-01-01 00:00:00 UTC'))
   and td.effective_from_dt < ifnull(price.discount_enddate,timestamp('2999-12-31 23:59:59 UTC'))
 where (ifnull(td.activation_date,ifnull(td.termination_date,timestamp('2999-12-31 23:59:59 UTC'))) <= td.effective_from_dt 
   and td.effective_from_dt <= ifnull(td.termination_date,timestamp('2999-12-31 23:59:59 UTC')))
   and (   ifnull(price.discount,0) != 0
        or td.discount_type          = 'PRICE PROTECTION');
       
------------------------------------------------------------------------------------------------------------
-- Precurssor calculations before offer stacking starts. 
-- And filtering data where discount is !=0 or discount_type = 'PRICE PROTECTION
------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       null as max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       td.discount_version,
       row_number() over (partition by discounted_product_event_id
                              order by discount_priority,
                                       gross_flag,
                                       portfolio_offer_id) as offer_stack_rn,
       count(1) over (partition by discounted_product_event_id) as stack_offer_sum,
       sum(case
             when discount_type = 'PRICE PROTECTION' then
               1
             else
               0
           end) over ( partition by discounted_product_event_id) as price_protection_stack_sum,
       null as description,
       transaction_type,
       null as quotedprice
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p12 td;
 
-----------------------------------------------------------------------------------------------------------------------------   
-- This is the first stack calculation using the columns above. It is calculated in the 6 steps below , this being the first.
-- Is the initial query and doesn't require lagging
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
-- Offers are applied in the order defined by the discount priority
-- with a discount type of 'VALUE' deduct a consistent monetary amount
-- Offers with a discount type of 'PERCENT' deduct a percentage of the product price.  
--The price used is dependant on the gross flag with a true value meaning the total product price is discounted and a 
-- false value meaning the sub total after other discounts have been applied is used.
-- Offers with a discount type of 'PRICE PROTECTION' discount a dynamic amount depending on the current product price. 
----------------------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_1:WRITE_TRUNCATE:
select portfolio_offer_id,       
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       quotedprice,
       greatest(case
          when not active_flag and offer_stack_rn = 1  then
            discounted_product_current_price
          when discount_type = 'VALUE' and offer_stack_rn = 1 then
             discounted_product_current_price - discount
          when discount_type = 'PERCENT' and offer_stack_rn = 1 and price_protection_stack_sum = 0 then
             discounted_product_current_price - (discounted_product_current_price*(discount/100))
          when discount_type = 'PERCENT' and offer_stack_rn = 1 and price_protection_stack_sum > 0 then
             discounted_product_starting_price - (discounted_product_starting_price*(discount/100))
          when discount_type = 'PRICE PROTECTION' and offer_stack_rn = 1 then
             discounted_product_starting_price
       end,0) charge_amount_net,
       discounted_product_current_price as charge_amount_gross
 from  uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13; 

-----------------------------------------------------------------------------------------------------
-- This is the second stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
-----------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_2:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       quotedprice,
       case
         when not active_flag and offer_stack_rn = 2 then greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0)
         when discount_type = 'PRICE PROTECTION' and offer_stack_rn = 2  then
           least(discounted_product_starting_price,greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0))
          when offer_stack_rn = 2 and price_protection_stack_sum > 0 then
            case
              when discount_type = 'VALUE' then
                        greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - discount,0)
              when discount_type = 'PERCENT' then
                                greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_starting_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_starting_price))*(discount/100)),0)
                         end
           when offer_stack_rn = 2 and price_protection_stack_sum = 0 then
               case
                 when discount_type = 'VALUE' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                     order by offer_stack_rn),discounted_product_current_price) - discount,0)
                 when discount_type = 'PERCENT' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                    order by offer_stack_rn),discounted_product_current_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_current_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_current_price))*(discount/100)),0)
                        end
                  else
           charge_amount_net
       end charge_amount_net,
       case
         when offer_stack_rn = 2 then
           greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                      order by offer_stack_rn),0)
         else
           greatest(charge_amount_gross,0)
       end as charge_amount_gross
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_1;
    
-----------------------------------------------------------------------------------------------  
-- This is the third stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM  
-----------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_3:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       quotedprice,
       case
         when not active_flag and offer_stack_rn = 3 then greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0)
         when discount_type = 'PRICE PROTECTION' and offer_stack_rn = 3 then
            least(discounted_product_starting_price,greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0))
          when offer_stack_rn = 3 and price_protection_stack_sum > 0 then
            case
              when discount_type = 'VALUE' then
                        greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - discount,0)
              when discount_type = 'PERCENT' then
                                greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_starting_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_starting_price))*(discount/100)),0)
                         end
           when offer_stack_rn = 3 and price_protection_stack_sum = 0 then
               case
                 when discount_type = 'VALUE' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                     order by offer_stack_rn),discounted_product_current_price) - discount,0)
                 when discount_type = 'PERCENT' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                    order by offer_stack_rn),discounted_product_current_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_current_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_current_price))*(discount/100)),0)
                        end
                  else
           charge_amount_net
       end charge_amount_net,
       case
         when offer_stack_rn = 3 then
           greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                      order by offer_stack_rn),0)
         else
           greatest(charge_amount_gross,0)
       end as charge_amount_gross
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_2;

-----------------------------------------------------------------------------------------------------------------------------  
-- This is the forth stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM  
-----------------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_4:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       quotedprice,
       case
         when not active_flag and offer_stack_rn = 4 then greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0)
         when discount_type = 'PRICE PROTECTION' and offer_stack_rn = 4 then
            least(discounted_product_starting_price,greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0))
          when offer_stack_rn = 4 and price_protection_stack_sum > 0 then
            case
              when discount_type = 'VALUE' then
                        greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - discount,0)
              when discount_type = 'PERCENT' then
                                greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_starting_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_starting_price))*(discount/100)),0)
                         end
           when offer_stack_rn = 4 and price_protection_stack_sum = 0 then
               case
                 when discount_type = 'VALUE' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                     order by offer_stack_rn),discounted_product_current_price) - discount,0)
                 when discount_type = 'PERCENT' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                    order by offer_stack_rn),discounted_product_current_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_current_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_current_price))*(discount/100)),0)
                        end
                  else
           charge_amount_net
       end charge_amount_net,
       case
         when offer_stack_rn = 4 then
           greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                      order by offer_stack_rn),0)
         else
           greatest(charge_amount_gross,0)
       end as charge_amount_gross
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_3;  
  
-------------------------------------------------------------------------------------------------------------------------------
-- This is the fifth stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM  
--------------------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_5:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       quotedprice,
       case
         when not active_flag and offer_stack_rn = 5 then greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0)
         when discount_type = 'PRICE PROTECTION' and offer_stack_rn = 5 then
            least(discounted_product_starting_price,greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0))
          when offer_stack_rn = 5 and price_protection_stack_sum > 0 then
            case
              when discount_type = 'VALUE' then
                        greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - discount,0)
              when discount_type = 'PERCENT' then
                                greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_starting_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_starting_price))*(discount/100)),0)
                         end
           when offer_stack_rn = 5 and price_protection_stack_sum = 0 then
               case
                 when discount_type = 'VALUE' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                     order by offer_stack_rn),discounted_product_current_price) - discount,0)
                 when discount_type = 'PERCENT' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                    order by offer_stack_rn),discounted_product_current_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_current_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_current_price))*(discount/100)),0)
                        end
                  else
           charge_amount_net
       end charge_amount_net,
       case
         when offer_stack_rn = 5 then
           greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                      order by offer_stack_rn),0)
         else
           greatest(charge_amount_gross,0)
       end as charge_amount_gross
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_4; 
  
-----------------------------------------------------------------------------------
-- This is the sixth stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
---------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_6:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       activation_date,
       termination_date,
       portfolio_offer_priceable_unit_id,
       discount_type,
       discount,
       max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       quotedprice,
       case
         when not active_flag and offer_stack_rn = 6 then greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0)
         when discount_type = 'PRICE PROTECTION' and offer_stack_rn = 6 then
            least(discounted_product_starting_price,greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                            order by offer_stack_rn),0))
          when offer_stack_rn = 6 and price_protection_stack_sum > 0 then
            case
              when discount_type = 'VALUE' then
                        greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - discount,0)
              when discount_type = 'PERCENT' then
                                greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                            order by offer_stack_rn),discounted_product_starting_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_starting_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_starting_price))*(discount/100)),0)
                         end
           when offer_stack_rn = 6 and price_protection_stack_sum = 0 then
               case
                 when discount_type = 'VALUE' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                     order by offer_stack_rn),discounted_product_current_price) - discount,0)
                 when discount_type = 'PERCENT' then
                                   greatest(ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                    order by offer_stack_rn),discounted_product_current_price) - (if(gross_flag or transaction_type <> 'RC',discounted_product_current_price,ifnull(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                                                                                                                                                                                                                                                order by offer_stack_rn),discounted_product_current_price))*(discount/100)),0)
                        end
                  else
           charge_amount_net
       end charge_amount_net,
       case
         when offer_stack_rn = 6 then
           greatest(lag(charge_amount_net) over ( partition by discounted_product_event_id
                                                      order by offer_stack_rn),0)
         else
           greatest(charge_amount_gross,0)
       end as charge_amount_gross
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_5;
       
---------------------------------------------------------------------------------------------------------------------------
-- FINAL td table lagging effective_from_dts to create the final arrangement for EFFECTIVE_TO_DT
---------------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_soip:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       lag(effective_from_dt,
           1,
           timestamp('2999-12-31 23:59:59 UTC')) over (partition by portfolio_offer_id
                                                           order by effective_from_dt         desc,
                                                                    effective_from_dt_csn_seq desc,
                                                                    effective_from_dt_seq     desc) as effective_to_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       cast(null as string)                 as original_portfolio_offer_id,
       portfolio_offer_priceable_unit_id,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_current_price     as discounted_product_price,
       currency,
       active_flag,
       status_code,
       gross_flag,
       discount_id,
       discount_type,
       discount_priority,
       discount_version,
       offer_stack_rn,
       stack_offer_sum,
       price_protection_stack_sum,
       description,
       transaction_type,
       discount,
       charge_amount_gross,
       charge_amount_net,
       case 
         when offer_stack_rn > 6 then
           0
         else
           greatest(ifnull(charge_amount_gross,0) - ifnull(charge_amount_net,0),0) 
       end discount_value
  from uk_pre_customer_spine_offer_is.td_offer_discount_soip_p13_6;
 