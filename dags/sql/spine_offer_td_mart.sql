--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_td_mart.sql
-- Author        : Ankita Roy
-- Date Created  : 14th Feb 2022
--
--------------------------------------------------------------------------------
--
-- Description   : Populate TD_MART_OFFER as a precursor to populating
--                 MART_OFFER 
--
-- Comments      : This script populates the mart table for offer
--                                        
-- Usage         : Standard BQSQL Call
--
-- Called By     : spine_offer_mart.sh
--
-- Calls         : none.
--
-- Parameters    : NA
--
-- Exit codes    : 0 - Success
--                 1 - Failure
--
-- Revisions
-- =============================================================================
-- Date     userid  MR#       Comments                                      Ver.
-- ------   ------  ------    --------------------------------------------  ----
-- 220222   ary17   Spine     initial version                               1.0
--------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
-- Getting relevant data from the 2 source tables(lkp_catalogue_product and lkp_entitlement
-- for type1 columns
------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_mart_offer_p1a:WRITE_TRUNCATE:
select lcp.id                                                              id,
       lcp.bill_name                                                       bill_name,
       lcp.name                                                            name,
       lcp.type                                                            type,
       lcp.transaction_type                                                transaction_type,
       lkpe.code                                                           code
  from uk_pub_cust_spine_shared_is.lkp_catalogue_product lcp
 left outer join uk_pub_cust_spine_shared_is.lkp_entitlement lkpe
    on (lkpe.id = lcp.entitlement_id);


--------------------------------------------------------------------------------
-- Pulling type 1 data from fact_offer table and populating into p1
-- Self join  of fact_offer to pull the first records based on first_portfolio_offer_id
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_mart_offer_p1:WRITE_TRUNCATE:
  select fact.id                                                 portfolio_offer_id,
       fact.created_dt                                           created_dt,
       fact.created_by_id                                        created_by_id,
       fact.portfolio_id                                         portfolio_id,
       fact.billing_account_id                                   billing_account_id,
       fact.account_number                                       account_number,
       fact.account_type                                         account_type,
       fact.currency_code                                        currency_code,
       fact.service_instance_id                                  service_instance_id,
       fact.order_id                                             order_id,
       fact.version                                              version,
       fact.previous_portfolio_offer_id                          previous_portfolio_offer_id,
       fact.first_portfolio_offer_id                             first_portfolio_offer_id,
       ft.order_id                                               first_portfolio_offer_order_id,
       ft.created_by_id                                          first_portfolio_offer_created_by_id,
       ft.created_dt                                             first_portfolio_offer_created_dt,
       fact.offer_detail_id                                      offer_detail_id, 
       fact.code                                                 offer_code,
       fact.name                                                 offer_name,
       fact.bill_name                                            offer_bill_name,
       fact.transaction_type                                     offer_transaction_type,
       fact.pac                                                  pac,
       fact.gross_flag                                           gross_flag,
       fact.automatic_flag                                       automatic_flag,
       fact.discount_duration                                    offer_duration,
       fact.start_dt                                             offer_start_dt,
       fact.end_dt                                               offer_end_dt
  from uk_pub_customer_spine_offer_is.fact_offer fact
  left outer join uk_pub_customer_spine_offer_is.fact_offer ft
   on (fact.first_portfolio_offer_id = ft.id);


-------------------------------------------------------------------------------
-- Pulling relevant fields from dim_offer_status table to maintain history later
-------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_lkp1:WRITE_TRUNCATE:
select to_base64(sha512(concat(p1.portfolio_offer_id,
                               dos.effective_from_dt,
                               dos.effective_from_dt_csn_seq,
                               dos.effective_from_dt_seq
                                               )))                                   index_hash,
        p1.portfolio_offer_id                                                        portfolio_offer_id,
        dos.effective_from_dt                                                        effective_from_dt,
        dos.effective_from_dt_csn_seq                                                effective_from_dt_csn_seq,
        dos.effective_from_dt_seq                                                    effective_from_dt_seq,
        dos.status_code                                                              status_code,
        lag(dos.status_code,1) over (partition by dos.portfolio_offer_id
                                              order by dos.effective_from_dt,
                                                       dos.effective_from_dt_csn_seq,
                                                       dos.effective_from_dt_seq)   prev_stat_code,
        dos.status                                                                  status,
        dos.reason_code                                                             status_reason_code,
        dos.reason                                                                  status_reason,
        dos.effective_from_dt                                                       status_from_dt,
        dos.effective_to_dt                                                         status_to_dt,
        cast(null as integer)                                                       dt_index,
        cast(null as integer)                                                       next_dt_index
   from uk_pub_customer_spine_offer_is.dim_offer_status  dos
  inner join  uk_pre_customer_spine_offer_is.td_mart_offer_p1 p1
     on (p1.portfolio_offer_id   = dos.portfolio_offer_id);


-------------------------------------------------------------------------------
-- Derivation of fields activation_dt and terminated_dt from dim_offer_status
-------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_lkp:WRITE_TRUNCATE:
select doslk.index_hash                                index_hash,
        doslk.portfolio_offer_id                       portfolio_offer_id,
        doslk.effective_from_dt                        effective_from_dt,
        doslk.effective_from_dt_csn_seq                effective_from_dt_csn_seq,
        doslk.effective_from_dt_seq                    effective_from_dt_seq,
        doslk.status_code                              status_code,
        doslk.prev_stat_code                           prev_stat_code,
        ad.activation_dt                               activation_dt,
        td.terminated_dt                               terminated_dt,
        doslk.status                                   status,
        doslk.status_reason_code                       status_reason_code,
        doslk.status_reason                            status_reason,
        doslk.status_from_dt                           status_from_dt,
        doslk.status_to_dt                             status_to_dt,
        doslk.dt_index                                 dt_index,
        doslk.next_dt_index                            next_dt_index
   from uk_pre_customer_spine_offer_is.td_offer_status_lkp1 doslk
  left outer join (select min(effective_from_dt) as activation_dt,
                          portfolio_offer_id,
                      from uk_pre_customer_spine_offer_is.td_offer_status_lkp1
                    where upper(status_code) = 'ACT'
                      group by portfolio_offer_id) ad
       on (ad.portfolio_offer_id = doslk.portfolio_offer_id)
  left outer join (select max(effective_from_dt) as terminated_dt,
                          portfolio_offer_id,
                      from uk_pre_customer_spine_offer_is.td_offer_status_lkp1
                    where upper(status_code) = 'TMD'
                    and ifnull(prev_stat_code, '?') != 'TMD'
                      group by portfolio_offer_id) td
        on (td.portfolio_offer_id = doslk.portfolio_offer_id)
   where not(ifnull(doslk.status_code, '?') = 'TMD'
  and ifnull(doslk.prev_stat_code, '?') = 'TMD');


-------------------------------------------------------------------------------
--Pulling relevant fields from dim_offer_discount table to maintain history later 
-------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_lkp:WRITE_TRUNCATE:
select to_base64(sha512(concat(p1.portfolio_offer_id,
                               dod.effective_from_dt,
                               '-1',
                               '-1')))                 index_hash,
        p1.portfolio_offer_id                          portfolio_offer_id,
        dod.effective_from_dt                          effective_from_dt,
        cast(-1 as numeric)                            effective_from_dt_csn_seq,
        cast(-1 as numeric)                            effective_from_dt_seq,
        dod.discounted_product_event_id                discounted_product_event_id,
        dod.discounted_priceable_unit_id               discounted_priceable_unit_id,
        dod.discounted_product_id                      catalogue_product_id,
        dod.discounted_product_price                   catalogue_product_price,
        dod.discount_priority                          discount_priority,
        dod.discount_type                              discount_type,
        dod.discount                                   discount,
        dod.discount_value                             discount_value,
        dod.portfolio_offer_priceable_unit_id          portfolio_offer_priceable_unit_id,
        cast(dod.discount_version as string)           discount_version,
        cast(null as integer)                          dt_index,
        cast(null as integer)                          next_dt_index
   from uk_pub_customer_spine_offer_is.dim_offer_discount dod
  inner join  uk_pre_customer_spine_offer_is.td_mart_offer_p1 p1
     on (p1.portfolio_offer_id   = dod.portfolio_offer_id);


-----------------------------------------------------------------------------------------------
--create an index table by doing union of index and date fields from the 2 history tables 
--(dim_offer_status and dim_offer_discount)
-----------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_index_p1:WRITE_TRUNCATE:
select index_hash,
       portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq
  from uk_pre_customer_spine_offer_is.td_offer_status_lkp
 union distinct
 select index_hash,
        portfolio_offer_id,
        effective_from_dt,
       cast(-1 as numeric)  effective_from_dt_csn_seq,
       cast(-1 as numeric)  effective_from_dt_seq
  from uk_pre_customer_spine_offer_is.td_offer_discount_lkp;


--------------------------------------------------------------------------------------------
--This will assign index(dt_index) in sequential order  to each distinct date for a
-- particular portfolio_offer_id
--------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_mart_index:WRITE_TRUNCATE:
select index_hash,
       portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       dt_index,
       max(dt_index) over (partition by portfolio_offer_id) max_dt_index
  from (select index_hash,
               portfolio_offer_id,
               effective_from_dt,
               effective_from_dt_csn_seq,
               effective_from_dt_seq,
               row_number() over (partition by portfolio_offer_id
                                      order by effective_from_dt,
                                               effective_from_dt_csn_seq,
                                               effective_from_dt_seq)
                                                                  dt_index
          from uk_pre_customer_spine_offer_is.td_offer_index_p1);


-----------------------------------------------------------------------------------------
--This will update the dim_offer_status lkp table with dt_index and next_dt_index values
--next_dt_index creates the upper bound for history to be merged later
-----------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_lkp:UPDATE:
update uk_pre_customer_spine_offer_is.td_offer_status_lkp trg
   set trg.dt_index       = src.dt_index,
       trg.next_dt_index = src.next_dt_index
  from (select index_hash,
               dt_index,
               ifnull(next_dt_index, max_dt_index + 1) next_dt_index
          from (select  lkp.index_hash,
                        ix.dt_index,
                        lead(ix.dt_index,1) over (partition by lkp.portfolio_offer_id
                                                      order by lkp.effective_from_dt,
                                                               lkp.effective_from_dt_csn_seq,
                                                               lkp.effective_from_dt_seq) next_dt_index,
                        ix.max_dt_index
                   from uk_pre_customer_spine_offer_is.td_offer_status_lkp lkp
                   left join uk_pre_customer_spine_offer_is.td_offer_mart_index ix
                     on lkp.index_hash = ix.index_hash)) src
 where trg.index_hash = src.index_hash;


-------------------------------------------------------------------------------------------
--This will update the dim_offer_discount lkp table with dt_index and next_dt_index values
--next_dt_index creates the upper bound for history to be merged later
-------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_lkp:UPDATE:
update uk_pre_customer_spine_offer_is.td_offer_discount_lkp trg
   set trg.dt_index       = src.dt_index,
       trg.next_dt_index = src.next_dt_index
  from (select index_hash,
               dt_index,
               ifnull(next_dt_index, max_dt_index + 1) next_dt_index
          from (select  lkp.index_hash,
                        ix.dt_index,
                        lead(ix.dt_index,1) over (partition by lkp.portfolio_offer_id
                                                      order by lkp.effective_from_dt,
                                                               lkp.effective_from_dt_csn_seq,
                                                               lkp.effective_from_dt_seq) next_dt_index,
                        ix.max_dt_index
                   from uk_pre_customer_spine_offer_is.td_offer_discount_lkp lkp
                   left join uk_pre_customer_spine_offer_is.td_offer_mart_index ix
                     on lkp.index_hash = ix.index_hash)) src
 where trg.index_hash = src.index_hash;


-------------------------------------------------------------------------------------------------------------
-- Joining index table created above with the fact_offer based on portfolio_offer_id
-- and then further join with the history tables dim_offer_status and dim_offer_discount to maintain history
--Only dates which are greater than or equal to created_dt of fact_offer are pulled through from the index.
--The history is populated till the terminatted_dt from dim_offer_status
----------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_mart_offer_p2:WRITE_TRUNCATE:
select dx.index_hash                                   index_hash,
       p1.portfolio_offer_id                           portfolio_offer_id,
       dx.effective_from_dt                            effective_from_dt,
       dx.effective_from_dt_csn_seq                    effective_from_dt_csn_seq,
       dx.effective_from_dt_seq                        effective_from_dt_seq,
       lead(dx.effective_from_dt,1) over (partition by p1.portfolio_offer_id
                                              order by dx.effective_from_dt,
                                                       dx.effective_from_dt_csn_seq,
                                                       dx.effective_from_dt_seq)         next_effective_from_dt,
       dx.dt_index                                     dt_index,
       p1.created_dt                                   created_dt,
       p1.created_by_id                                created_by_id,
       od.discounted_product_event_id                  discounted_product_event_id,
       p1.portfolio_id                                 portfolio_id,
       p1.billing_account_id                           billing_account_id,
       p1.account_number                               account_number,
       p1.account_type                                 account_type,
       p1.currency_code                                currency_code,
       p1.service_instance_id                          service_instance_id,
       dspu.subscription_id                            subscription_id,
       od.discounted_priceable_unit_id                 discounted_priceable_unit_id,
       dspu.type_code                                  discounted_priceable_unit_type_code,
       dspu.type                                       discounted_priceable_unit_type,
       od.catalogue_product_id                         catalogue_product_id,
       pla.bill_name                                   catalogue_product_bill_name,
       od.catalogue_product_price                      catalogue_product_price,
       pla.name                                        catalogue_product_name,
       pla.type                                        catalogue_product_type,
       pla.transaction_type                            catalogue_product_transaction_type,
       pla.code                                        entitlement_code,
       p1.order_id                                     order_id,
       p1.version                                      version,
       p1.previous_portfolio_offer_id                  previous_portfolio_offer_id,
       p1.first_portfolio_offer_id                     first_portfolio_offer_id,
       p1.first_portfolio_offer_order_id               first_portfolio_offer_order_id,
       p1.first_portfolio_offer_created_by_id          first_portfolio_offer_created_by_id,
       p1.first_portfolio_offer_created_dt             first_portfolio_offer_created_dt,
       case 
         when od.discount_version is null then
           p1.version
         else
           od.discount_version
       end                                             offer_version,
       p1.offer_detail_id                              offer_detail_id,
       p1.offer_code                                   offer_code,
       p1.offer_name                                   offer_name,
       p1.offer_bill_name                              offer_bill_name,
       p1.offer_transaction_type                       offer_transaction_type,
       p1.pac                                          pac,
       od.discount_priority                            discount_priority,
       od.discount_type                                discount_type,
       od.discount                                     discount,
       od.discount_value                               discount_value,
       dopu.quoted_discount                            quoted_discount,
       p1.gross_flag                                   gross_flag,
       p1.automatic_flag                               automatic_flag,
       os.status_code                                  status_code,
       lag(os.status_code,1) over (partition by p1.portfolio_offer_id
                                              order by dx.effective_from_dt,
                                                       dx.effective_from_dt_csn_seq,
                                                       dx.effective_from_dt_seq)        prev_code,
       os.status                                       status,
       os.status_reason_code                           status_reason_code,
       os.status_reason                                status_reason,
       os.status_from_dt                               status_from_dt,
       os.status_to_dt                                 status_to_dt,
       p1.offer_duration                               offer_duration,
       p1.offer_start_dt                               offer_start_dt,
       p1.offer_end_dt                                 offer_end_dt,
       os.activation_dt                                activation_dt,
       os.terminated_dt                                terminated_dt
  from uk_pre_customer_spine_offer_is.td_offer_mart_index dx
 inner  join uk_pre_customer_spine_offer_is.td_mart_offer_p1 p1
    on (dx.portfolio_offer_id = p1.portfolio_offer_id)
  left outer join uk_pre_customer_spine_offer_is.td_offer_status_lkp os
    on (    p1.portfolio_offer_id  = os.portfolio_offer_id
        and dx.dt_index  >= os.dt_index
        and dx.dt_index  <  os.next_dt_index)
  left outer join uk_pre_customer_spine_offer_is.td_offer_discount_lkp od
    on (    p1.portfolio_offer_id  = od.portfolio_offer_id
        and dx.dt_index  >= od.dt_index
        and dx.dt_index  <  od.next_dt_index)
  left outer join uk_pre_customer_spine_offer_is.td_mart_offer_p1a pla
   on (pla.id = od.catalogue_product_id)
  left outer join uk_pub_cust_spine_subs_is.dim_subscription_priceable_unit dspu
    on (dspu.id = od.discounted_priceable_unit_id)
  left outer join uk_pub_customer_spine_offer_is.dim_offer_priceable_unit dopu
    on (dopu.id = od.portfolio_offer_priceable_unit_id)
 where dx.effective_from_dt >= p1.created_dt
   and dx.effective_from_dt <= ifnull(os.terminated_dt, timestamp('2999-12-31 23:59:59'));


-------------------------------------------------------------------------------
-- Deriving history_order and all required previous fields.
-------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_mart_offer_p3:WRITE_TRUNCATE:
select to_base64(sha512(concat(p2.portfolio_offer_id,
                               p2.effective_from_dt,
                               p2.dt_index))) uniq_hash,
       p2.portfolio_offer_id,
       p2.effective_from_dt,
       p2.next_effective_from_dt,
       p2.effective_from_dt_csn_seq,
       p2.effective_from_dt_seq,
       cast(null as timestamp) effective_to_dt,
       p2.created_dt,
       ifnull(lead(p2.created_dt,1) over (partition by p2.first_portfolio_offer_id
                                              order by p2.portfolio_offer_id,
                                              p2.effective_from_dt,
                                              p2.effective_from_dt_csn_seq,
                                              p2.effective_from_dt_seq asc), p2.created_dt)   next_created_dt,
       p2.created_by_id,
       p2.dt_index,
       row_number() over (partition by p2.portfolio_offer_id 
                              order by p2.effective_from_dt,
                                       p2.dt_index)   history_order,
       p2.discounted_product_event_id,
       p2.portfolio_id,
       p2.billing_account_id,
       p2.account_number,
       p2.account_type,
       p2.currency_code,
       p2.service_instance_id,
       p2.subscription_id,
       p2.discounted_priceable_unit_id,
       p2.discounted_priceable_unit_type_code,
       p2.discounted_priceable_unit_type,
       p2.catalogue_product_id,
       p2.catalogue_product_bill_name,
       p2.catalogue_product_price,
       lag(p2.catalogue_product_price,1) over (partition by p2.portfolio_offer_id
                                                    order by p2.effective_from_dt,
                                                             p2.effective_from_dt_csn_seq,
                                                             p2.effective_from_dt_seq)       prev_catalogue_product_price,
       p2.catalogue_product_name,
       p2.catalogue_product_type,
       p2.catalogue_product_transaction_type,
       p2.entitlement_code,
       p2.order_id,
       p2.previous_portfolio_offer_id,
       p2.first_portfolio_offer_id,
       p2.first_portfolio_offer_order_id,
       p2.first_portfolio_offer_created_by_id,
       p2.first_portfolio_offer_created_dt,
       p2.offer_version,
       p2.offer_detail_id,
       p2.offer_code,
       p2.offer_name,
       p2.offer_bill_name,
       p2.offer_transaction_type,
       p2.pac,
       p2.discount_priority,
       p2.discount_type,
       p2.discount,
       p2.discount_value,
       p2.quoted_discount,
       p2.gross_flag,
       p2.automatic_flag,
       p2.status_code,
       lag(p2.status_code,1) over (partition by p2.portfolio_offer_id
                                              order by p2.effective_from_dt,
                                                       p2.effective_from_dt_csn_seq,
                                                       p2.effective_from_dt_seq)                 previous_status_code,
       p2.prev_code,
       p2.status,
       lag(p2.status,1) over (partition by p2.portfolio_offer_id
                                              order by p2.effective_from_dt,
                                                       p2.effective_from_dt_csn_seq,
                                                       p2.effective_from_dt_seq)                 previous_status,
       p2.status_reason_code,
       lag(p2.status_reason_code,1) over (partition by p2.portfolio_offer_id
                                              order by p2.effective_from_dt,
                                                       p2.effective_from_dt_csn_seq,
                                                       p2.effective_from_dt_seq)                 previous_status_reason_code,
       p2.status_reason,
       lag(p2.status_reason,1) over (partition by p2.portfolio_offer_id
                                              order by p2.effective_from_dt,
                                                       p2.effective_from_dt_csn_seq,
                                                       p2.effective_from_dt_seq)                 previous_status_reason,
       p2.status_from_dt,
       lag(p2.status_from_dt,1) over (partition by p2.portfolio_offer_id
                                              order by p2.effective_from_dt,
                                                       p2.effective_from_dt_csn_seq,
                                                       p2.effective_from_dt_seq)                 previous_status_from_dt,
       p2.status_to_dt,
       lag(p2.status_to_dt,1) over (partition by p2.portfolio_offer_id
                                              order by p2.effective_from_dt,
                                                       p2.effective_from_dt_csn_seq,
                                                       p2.effective_from_dt_seq)                 previous_status_to_dt,
       p2.offer_duration,
       p2.offer_start_dt,
       p2.offer_end_dt,
       p2.activation_dt,
       p2.terminated_dt
  from uk_pre_customer_spine_offer_is.td_mart_offer_p2 p2
  where p2.service_instance_id is not null
  and p2.status_code is not null
  and (ifnull(p2.effective_from_dt, timestamp('2999-12-31 23:59:59')) != ifnull(p2.next_effective_from_dt, timestamp('2999-12-31 23:59:59'))
    or (ifnull(p2.effective_from_dt, timestamp('2999-12-31 23:59:59')) = ifnull(p2.next_effective_from_dt, timestamp('2999-12-31 23:59:59'))
        and ifnull(p2.status_code, '?') != ifnull(p2.prev_code, '?')));


-------------------------------------------------------------------------------
--Final td table 
--Deriving all the flag and date columns needed
-------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_mart_offer:WRITE_TRUNCATE:
select p3.uniq_hash,
       p3.portfolio_offer_id,
       p3.effective_from_dt,
       p3.next_effective_from_dt,
       cast(null as timestamp) effective_to_dt,
       p3.created_dt,
       p3.next_created_dt,
       p3.created_by_id,
       p3.dt_index,
       p3.history_order,
       p3.discounted_product_event_id,
       p3.portfolio_id,
       p3.billing_account_id,
       p3.account_number,
       p3.account_type,
       p3.currency_code,
       p3.service_instance_id,
       p3.subscription_id,
       p3.discounted_priceable_unit_id,
       p3.discounted_priceable_unit_type_code,
       p3.discounted_priceable_unit_type,
       p3.catalogue_product_id,
       p3.catalogue_product_bill_name,
       p3.catalogue_product_price,
       p3.prev_catalogue_product_price,
       p3.catalogue_product_name,
       p3.catalogue_product_type,
       p3.catalogue_product_transaction_type,
       p3.entitlement_code,
       p3.order_id,
       p3.previous_portfolio_offer_id,
       p3.first_portfolio_offer_id,
       p3.first_portfolio_offer_order_id,
       p3.first_portfolio_offer_created_by_id,
       p3.first_portfolio_offer_created_dt,
       case 
            when p3.created_dt <> p3.next_created_dt then
              false
            else
              true
       end                                current_auto_transfer_offer_flag,
       p3.offer_version,
       p3.offer_detail_id,
       p3.offer_code,
       p3.offer_name,
       p3.offer_bill_name,
       p3.offer_transaction_type,
       p3.pac,
       p3.discount_priority,
       p3.discount_type,
       p3.discount,
       p3.discount_value,
       p3.quoted_discount,
       p3.gross_flag,
       p3.automatic_flag,
       p3.status_code,
       p3.previous_status_code,
       p3.prev_code,
       p3.status,
       p3.previous_status,
       p3.status_reason_code,
       p3.previous_status_reason_code,
       p3.status_reason,
       p3.previous_status_reason,
       p3.status_from_dt,
       p3.previous_status_from_dt,
       p3.status_to_dt,
       p3.previous_status_to_dt,
       case when (p3.status_code <> ifnull(p3.previous_status_code, '?')
                  and p3.previous_status_code is not null) then
             false
            else
             true
        end                                                status_changed_flag,
       case when (ifnull(p3.catalogue_product_price, -1) <> ifnull(p3.prev_catalogue_product_price, -1) 
                      or p3.catalogue_product_price is null) then
             true
            else
             false
        end                                                price_changed_flag,
       case when upper(p3.status_code) in('ACT','BLK','PTM') then
            true
           else
            false
        end                                                active_flag,
       p3.offer_duration,
       p3.offer_start_dt,
       p3.offer_end_dt,
       case when p3.effective_from_dt >= p3.activation_dt then 
           p3.activation_dt
        else 
           null
        end                                       activation_dt,
       case when (p3.terminated_dt = p3.effective_from_dt
                   and p3.status_code = 'TMD') then
           p3.terminated_dt
       else 
           null
        end                                       terminated_dt
  from uk_pre_customer_spine_offer_is.td_mart_offer_p3 p3;


-------------------------------------------------------------------------------
-- calculate effective_to_dt
-------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_mart_offer:UPDATE:
update uk_pre_customer_spine_offer_is.td_mart_offer trg
   set trg.effective_to_dt = src.effective_to_dt
  from (select uniq_hash,
               lag(effective_from_dt,
                   1,
                   timestamp('2999-12-31 23:59:59')
                  ) over (partition by portfolio_offer_id
                              order by effective_from_dt desc,
                                       history_order desc) effective_to_dt
          from uk_pre_customer_spine_offer_is.td_mart_offer
       ) src
 where trg.uniq_hash = src.uniq_hash;


