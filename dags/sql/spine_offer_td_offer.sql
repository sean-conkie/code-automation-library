--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_td_offer.sql
-- Author        : Emanuel Blei
-- Date Created  : 19th Aug 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate TD_FACT_OFFER
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
-- 190821   ebi08    BQ       Initial Version for GCP Big Query             1.0
-- 271021   ebi08    BQ       New version incorporating dim_offer_discount  1.1
--                            and link_campaignsource_offer as source
--                            tables.
--                            Changed transformation of table p1c from 
--                            row_num to array_agg for better performance.
-- 210122   ary17    BQ       firstportfolioofferid and pac changes          1.2
-- 070222   ary17    BQ       bug fixes                                      1.3
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- The service instance table needs to be joined to itself to establish a link 
-- between child, parent, and grandparent records.
-- This will create a link between the child id and the service instance id.
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_p1a:WRITE_TRUNCATE:
select child.id                                     id,
       child.serviceinstancetype                    service_instance_type,
       case
         when child.serviceinstancetype = 300 then
           child.id
         when parent.serviceinstancetype = 300 then
           parent.id
         when grandparent.serviceinstancetype = 300 then
           grandparent.id
         else 
           null
       end                                          billing_service_instance,
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbserviceinstance            child
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_bsbserviceinstance parent
    on (    child.parentserviceinstanceid = parent.id
        and parent.logically_deleted = 0)
  left outer join uk_tds_chordiant_eod_is.cc_chordiant_bsbserviceinstance grandparent
    on (    parent.parentserviceinstanceid = grandparent.id
        and grandparent.logically_deleted = 0)
 where child.logically_deleted = 0;


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Create link to bsbbillingaccount over bsbpriceableunit where we select the
-- first billingaccountid for each portfolioofferid.
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_p1b:WRITE_TRUNCATE:
select pu.portfolioofferid             portfolioofferid,
       ba.portfolioid                  portfolioid,
       ba.id                           id,
       ba.accountnumber                accountnumber,
       ba.currencycode                 currencycode,
       ba.organisation_unit            organisation_unit
  from (select portfolioofferid,
               billingaccountid
          from (select portfolioofferid, 
                       array_agg(billingaccountid ignore nulls
                                 order by effectivedate asc,
                                          created asc,
                                          id asc
                                 limit 1) arr
                  from uk_tds_chordiant_eod_is.cc_chordiant_bsbpriceableunit
                 where logically_deleted = 0
                 group by portfolioofferid),
               unnest(arr) billingaccountid
       )                                                           pu
 inner join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount ba
    on (    pu.billingaccountid = ba.id
        and ba.portfolioid is not null
        and ba.logically_deleted = 0);


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Select first record from orderline table when grouped by portfolioofferid
-- and ordered by created and id column.
-- Create logic for boolean automatic flag column.
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_p1c:WRITE_TRUNCATE:
select ol.portfolioofferid portfolio_offer_id,
       ol.created          created_dt,
       ol.createdby        created_by_id,
       ol.lastupdate       last_modified_dt,
       ol.updatedby        last_modified_by_id,
       ol.orderid          order_id,
       ol.id               order_line_id,
       o.urn               order_urn,
       case
         when ol.isautomaticoffer = 1 then
           true
         else 
           false
       end                 automatic_flag,
       o.interestsourceid  order_tbl_interestsourceid,
       o.created           order_tbl_created
  from (select portfolioofferid,
               first_row.created          created,
               first_row.createdby        createdby,
               first_row.lastupdate       lastupdate,
               first_row.updatedby        updatedby,
               first_row.orderid          orderid,
               first_row.id               id,
               first_row.isautomaticoffer isautomaticoffer
          from (select portfolioofferid,
                       array_agg(struct (created,
                                         createdby,
                                         lastupdate,
                                         updatedby,
                                         orderid,
                                         id,
                                         isautomaticoffer)
                                 ignore nulls 
                                  order by created asc,
                                           id asc
                                  limit 1) arr
                  from uk_tds_chordiant_eod_is.cc_chordiant_bsborderline t
                 where (    logically_deleted = 0
                       and action = 'AD')
                 group by portfolioofferid),
        unnest (arr) first_row)                                ol
 inner join uk_tds_chordiant_eod_is.cc_chordiant_bsborder o
            on (    ol.orderid = o.id
                and o.status <> 'APPCAN'
                and o.logically_deleted = 0);


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Extract relevant columns from dim_offer_type and create flag columns with 
-- conditional logic after aggregation by offer detail id.
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_p1d:WRITE_TRUNCATE:
select offer_detail_id,
       logical_or(case
                    when (   code = 'PFRZE'
                          or description = 'Price Hold - Level 1'
                         ) then
                      true
                    else 
                      false
                  end
                 ) price_freeze_offer_flag,
       logical_or(case
                    when code in ('STRC', 'ENTIN') then
                      true
                    else
                      false
                  end
                 ) in_contract_offer_flag
  from uk_pub_customer_spine_offer_is.dim_offer_type
 where offer_detail_id is not null
 group by offer_detail_id;


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Fix timestamps in link_campaignsource_offer so that effectivetodate 
-- conincides with effectivefromdate of next period.
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_p1e:WRITE_TRUNCATE:
select campaignsourceid,
       offerid,
       PAC,
       effectivefromdate,
       ifnull(lead(effectivefromdate) over (partition by campaignsourceid,
                                                         offerid
                                                order by effectivefromdate asc),
              effectivetodate) effectivetodate
  from uk_tds_refdata_eod_is.cc_refdata_link_campaignsource_offer
 where rdmdeletedflag = 'N';

--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Self join of bsbportfoliooffer table to get first_portfolio_offer_id
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_fact_offer_p1f:WRITE_TRUNCATE:
with recursive portfolio_hierarchy as
(
select a.id as id,
          a.origportfolioofferid as origportfolioofferid,
          a.id as first_portfolio_offer_id,
          1 as level
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbportfoliooffer a
 where a.origportfolioofferid is null
 and a.logically_deleted = 0
 union all
    select b.id as id,
           b.origportfolioofferid as origportfolioofferid,
           a.first_portfolio_offer_id as first_portfolio_offer_id,
           a.level + 1 as level
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbportfoliooffer b
 inner join portfolio_hierarchy a
    on (b.origportfolioofferid = a.id)
    where b.origportfolioofferid is not null
    and b.logically_deleted = 0
    and level < 100
)
select id,
       origportfolioofferid,
       first_portfolio_offer_id,
       level 
  from portfolio_hierarchy;

--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Build core table by joining the three driving tables bsbportfoliooffer, 
-- bsbserviceinstance and bsbbillingaccount.
--------------------------------------------------------------------------------

uk_pre_customer_spine_offer_is.td_fact_offer_p2:WRITE_TRUNCATE:
select po.id                                    id,
       po.origportfolioofferid                  previous_portfolio_offer_id,
       pof.first_portfolio_offer_id             first_portfolio_offer_id,
       pof.level                                hierarchy,
       po.offerid                               offer_detail_id,
       case
         when ba_pu.portfolioid is not null then
           ba_pu.portfolioid
         else
           ba_si.portfolioid
       end                                      portfolio_id,
       case
         when ba_pu.portfolioid is not null then
           ba_pu.id
         else
           ba_si.id
       end                                      billing_account_id,
       case
         when ba_pu.portfolioid is not null then
           ba_pu.accountnumber
         else
           ba_si.accountnumber
       end                                      account_number,
       po.serviceinstanceid                     service_instance_id,
       si.service_instance_type                 service_instance_type,
       case
         when ba_pu.portfolioid is not null then
           ba_pu.organisation_unit
         else
           ba_si.organisation_unit
       end                                      organisation_unit,
       case
         when ba_pu.portfolioid is not null then
           ba_pu.currencycode
         else
           ba_si.currencycode
       end                                      currency_code,
       ifnull(po.applicationstartdate,
              timestamp('1900-01-01'))          start_dt,
       ifnull(po.applicationenddate,
              timestamp('2999-12-31 23:59:59')) end_dt
  from uk_tds_chordiant_eod_is.cc_chordiant_bsbportfoliooffer      po
 left outer join uk_pre_customer_spine_offer_is.td_fact_offer_p1a       si
    on po.serviceinstanceid = si.id 
 left outer join uk_tds_chordiant_eod_is.cc_chordiant_bsbbillingaccount ba_si
    on (    si.billing_service_instance = ba_si.serviceinstanceid
        and ba_si.logically_deleted = 0)
  left outer join uk_pre_customer_spine_offer_is.td_fact_offer_p1b ba_pu
    on po.id = ba_pu.portfolioofferid 
 inner join uk_pre_customer_spine_offer_is.td_fact_offer_p1f pof
    on po.id = pof.id 
 where (    po.offerid is not null
        and po.logically_deleted = 0);


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Join p2 table with core columns to the remaining tables including a stand-in 
-- dummy for the dim_offer_discount table that needs to be added when available.
--------------------------------------------------------------------------------

uk_pre_customer_spine_offer_is.td_fact_offer_p1:WRITE_TRUNCATE:
select distinct p2.id                                                 id,
       p1c.created_dt                                        created_dt,
       ifnull(p1c.created_by_id, 'BATCH_USER')               created_by_id,
       p1c.last_modified_dt                                  last_modified_dt,
       ifnull(p1c.last_modified_by_id, 'BATCH_USER')         last_modified_by_id,
       p2.portfolio_id                                       portfolio_id,
       p2.billing_account_id                                 billing_account_id,
       p2.account_number                                     account_number,
       p2.service_instance_id                                service_instance_id,
       case
         when (    p2.service_instance_type in (210,220,100,400)
               and p2.organisation_unit = 10) then
           'CORE'
         when p2.service_instance_type in (610,620) then
           'MOBILE'
         when (    p2.service_instance_type in (100,400,500)
               and p2.organisation_unit = 20) then
           'NOW TV'
         else
           'OTHER'
       end                                                   account_type,
       p2.currency_code                                      currency_code,
       p1c.order_id                                          order_id,
       p1c.order_line_id                                     order_line_id,
       p2.previous_portfolio_offer_id                        previous_portfolio_offer_id,
       p2.first_portfolio_offer_id                           first_portfolio_offer_id,
       p2.hierarchy                                          hierarchy,
       dod.discount_id                                       discount_id,
       dod.discount_type                                     discount_type,
       dref.discountduration                                 discount_duration,
       dod.discount                                          discount,
       p2.start_dt                                           start_dt,
       p2.end_dt                                             end_dt,
       p1c.order_urn                                         order_urn,
       cast(p2.offer_detail_id as string)                    offer_detail_id,
       ro.code                                               code,
       ro.description                                        description,
       ro.offertitledescription                              name,
       ro.offerbillname                                      bill_name,
       ro.offer_type                                         transaction_type,
       ifnull(ro.pac, p1e.pac)                                pac,
       ro.version                                            version,
       case
         when dos.status_code in ('ACT', 'BLK', 'PTM') then
           true 
         else 
           false
       end                                                   active_flag,
       p1c.automatic_flag                                    automatic_flag,
       case 
         when (   ro.is_gross is null
               or ro.is_gross = 1) then
             true
         else 
             false
       end                                                   gross_flag,
       case 
         when ro.pac is not null then 
           true 
         else 
           false
       end                                                   global_flag,
       p1d.price_freeze_offer_flag                           price_freeze_offer_flag,
       p1d.in_contract_offer_flag                            in_contract_offer_flag

  from uk_pre_customer_spine_offer_is.td_fact_offer_p2                  p2
  left outer join uk_pre_customer_spine_offer_is.td_fact_offer_p1c      p1c
    on p2.id = p1c.portfolio_offer_id
  left outer join uk_pre_customer_spine_offer_is.td_fact_offer_p1d      p1d
    on p2.offer_detail_id = p1d.offer_detail_id
  left outer join uk_pub_customer_spine_offer_is.dim_offer_discount     dod
    on (    p2.id = dod.portfolio_offer_id
        and dod.effective_to_dt = '2999-12-31 23:59:59')
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsbofferdiscount     dref
    on (    dod.discount_id = dref.id
        and dref.rdmdeletedflag = 'N')
  left outer join uk_pub_customer_spine_offer_is.dim_offer_status       dos
    on (    p2.id = dos.portfolio_offer_id
        and dos.effective_to_dt = '2999-12-31 23:59:59')
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsboffer             ro
    on (    p2.offer_detail_id = ro.id
        and ro.rdmdeletedflag = 'N')
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsbinterestsource    bis
    on (    p1c.order_tbl_interestsourceid = bis.id
        and bis.rdmdeletedflag = 'N')
  left outer join uk_pre_customer_spine_offer_is.td_fact_offer_p1e      p1e
    on (    p1e.campaignsourceid = bis.campaignsourceid
        and p2.offer_detail_id = p1e.offerid
        and p1c.order_tbl_created >= p1e.effectivefromdate
        and p1c.order_tbl_created < p1e.effectivetodate);


--------------------------------------------------------------------------------
-- Write Disposition: WRITE_TRUNCATE
--------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
-- pac column derivation using first_portfolio_offer_id and populating td_fact_offer table
-------------------------------------------------------------------------------------------

uk_pre_customer_spine_offer_is.td_fact_offer:WRITE_TRUNCATE:
select p1.id                                                                              id,
       ifnull(ifnull(p1.created_dt, bpo.statuschangeddate),timestamp('1900-01-01'))       created_dt,
       p1.created_by_id                                                                   created_by_id,
       ifnull(ifnull(p1.last_modified_dt, bpo.statuschangeddate),timestamp('1900-01-01')) last_modified_dt,
       p1.last_modified_by_id                                                             last_modified_by_id,
       p1.portfolio_id                                                                    portfolio_id,
       p1.billing_account_id                                                              billing_account_id,
       p1.account_number                                                                  account_number,
       p1.service_instance_id                                                             service_instance_id,
       p1.account_type                                                                    account_type,
       p1.currency_code                                                                   currency_code,
       p1.order_id                                                                        order_id,
       p1.order_line_id                                                                   order_line_id,
       p1.previous_portfolio_offer_id                                                     previous_portfolio_offer_id,
       p1.first_portfolio_offer_id                                                        first_portfolio_offer_id,
       p1.hierarchy                                                                       hierarchy,
       p1.discount_id                                                                     discount_id,
       p1.discount_type                                                                   discount_type,
       p1.discount_duration                                                               discount_duration,
       p1.discount                                                                        discount,
       p1.start_dt                                                                        start_dt,
       p1.end_dt                                                                          end_dt,
       p1.order_urn                                                                       order_urn,
       p1.offer_detail_id                                                                 offer_detail_id,
       p1.code                                                                            code,
       p1.description                                                                     description,
       p1.name                                                                            name,
       p1.bill_name                                                                       bill_name,
       p1.transaction_type                                                                transaction_type,
       case
       when (p1.pac is null
         and p1.first_portfolio_offer_id <> p1.id) then
         p3.pac
       else
          p1.pac
        end                                                                               as pac,
       p1.version                                                                         version,
       p1.active_flag                                                                     active_flag,
       p1.automatic_flag                                                                  automatic_flag,
       p1.gross_flag                                                                      gross_flag,
       p1.global_flag                                                                     global_flag,
       p1.price_freeze_offer_flag                                                         price_freeze_offer_flag,
       p1.in_contract_offer_flag                                                          in_contract_offer_flag

  from uk_pre_customer_spine_offer_is.td_fact_offer_p1                  p1
  left outer join (select id,pac
                     from uk_pre_customer_spine_offer_is.td_fact_offer_p1 )p3
   on (p1.first_portfolio_offer_id = p3.id)
  left outer join  (select id,
                           min(statuschangeddate) as statuschangeddate
                      from uk_tds_chordiant_eod_fc_is.fc_chordiant_bsbportfoliooffer 
                      where logically_deleted = 0
                       group by id) bpo
   on (p1.id = bpo.id)  
   where p1.portfolio_id is not null
     and p1.billing_account_id is not null; 

