--------------------------------------------------------------------------------
--
-- Filename      : spine_offer_td_offer_discount_2.sql
-- Author        : Nivedita Sanil/ Craig McKeever
-- Date Created  : 06 Sept 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Truncate and repopulate the TD table's that will populate the
--                 Dimension table to holds a history of status for order lines
--                 Table provides a history of the monetary value an offer would 
--                 discount. This will contain the second section
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
-- ------   ------  ------    --------------------------------------------  ----
-- 060921   nsa19             Initial version                               1.0
-- 080921   cmc37             Peer Programming                              1.1
-- 091121   cmc37             Updates to stacking to remove default         1.2
-- 071121   nsa19   BDP-24837 Recalculate -ve discount value                1.3
-- 070222   acr73   BDCF-225  Adding filter 
--                            discount_type <> 'PRICE PROTECTION'           1.4
-- 180222   nsa19   BDP-24837 Recalculate -ve discount value                1.5
-- 070322   nsa19             0 discounted_product_price scenario           1.6
--------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
-- Recalculate active_flag for exisiting records from source and those for the generated records
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p8:WRITE_TRUNCATE:     
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
       end                                                             as source_field,
       ifnull(p.active_flag,latest_flag.active_flag)                   as active_flag,
       ifnull(p.status,latest_flag.status)                             as status_code,
       ifnull(p.previous_status,latest_flag.previous_status)           as previous_status,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_quoted_price,
       td.discount_id,
       td.currency
  from uk_pre_customer_spine_offer_is.td_offer_discount_p7 td
  left outer join uk_pre_customer_spine_offer_is.td_offer_discount_p3 p
    on td.portfolio_offer_id = p.portfolio_offer_id
   and td.effective_from_dt  = p.offer_statuschangeddate
  left outer join ( select portfolio_offer_id,
                           effective_from_dt,
                           effective_from_dt_csn_seq,
                           effective_from_dt_seq,
                           status,
                           previous_status,
                           active_flag
                      from (select 
                                   p7.portfolio_offer_id,
                                   p7.effective_from_dt,
                                   p3.effective_from_dt_csn_seq,
                                   p3.effective_from_dt_seq,
                                   p3.offer_statuschangeddate,
                                   p3.status,
                                   p3.previous_status,
                                   p3.active_flag,
                                   p3.termination_date,
                                   row_number() over (partition by p7.portfolio_offer_id,effective_from_dt
                                                          order by offer_statuschangeddate   desc,
                                                                   effective_from_dt         desc,
                                                                   effective_from_dt_csn_seq desc,
                                                                   effective_from_dt_seq     desc) as rn
                             from uk_pre_customer_spine_offer_is.td_offer_discount_p7 p7 
                             left outer join uk_pre_customer_spine_offer_is.td_offer_discount_p3 p3
                               on p3.portfolio_offer_id = p7.portfolio_offer_id
                              and p7.effective_from_dt > p3.offer_statuschangeddate
                              and p7.effective_from_dt < p3.termination_date
                             ) foo
                     where rn =1
                   ) latest_flag
    on td.portfolio_offer_id = latest_flag.portfolio_offer_id
   and td.effective_from_dt  = latest_flag.effective_from_dt;

   
------------------------------------------------------------------------------------------------------------
-- Default all null active_flags outside of activation and termination date as as false. Calculate price and
-- discount values.
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p9:WRITE_TRUNCATE:
select cdc.portfolio_offer_id,
       cdc.effective_from_dt,
       cdc.effective_from_dt_csn_seq,
       cdc.effective_from_dt_seq,
       cdc.created_dt,
       cdc.created_by_id,
       cdc.last_modified_dt,
       cdc.last_modified_by_id,
       cdc.activation_date,
       cdc.termination_date,
       cdc.portfolio_offer_priceable_unit_id,
       cdc.discounted_product_event_id,
       cdc.discounted_priceable_unit_id,
       cdc.discounted_product_id,
       cdc.source_field,
       cdc.active_flag,
       cdc.status_code,
       cdc.previous_status,
       cdc.original_portfolio_offer_id,
       cdc.offerid,
       cdc.discounted_product_starting_price,
       cdc.discounted_product_current_price,
       cdc.previous_current_price,
       cdc.discount_id,
       cdc.currency,
       cdc.discount_version,
       cdc.value
  from (
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
                to_base64(sha512(concat(td.discounted_priceable_unit_id, ':',
                                        td.effective_from_dt, ':'
                                        )
                                 )
                         )            as discounted_product_event_id,
                td.discounted_priceable_unit_id,
                td.discounted_product_id,
                td.source_field,
                case
                  when td.active_flag is null and (effective_from_dt < activation_date or effective_from_dt > termination_date ) then
                    false
                  else
                    td.active_flag
                end                                                   as active_flag,
                td.status_code,
                case
                  when td.previous_status is null then
                    lag(status_code,1) over (partition by portfolio_offer_id
                                                order by effective_from_dt)
                  else td.previous_status
                end as previous_status,
                td.original_portfolio_offer_id,
                td.offerid,
                ifnull(price2.price, td.discounted_quoted_price)       as discounted_product_starting_price,
                ifnull(price.price,0)                                  as discounted_product_current_price,
                lag(price.price,1) over (partition by price.catalogueproductid,td.portfolio_offer_id
                                          order by td.effective_from_dt) as previous_current_price ,
                td.discount_id,
                td.currency,
                ovd.versionnumber                                      as discount_version,
                ovd.value                                              as value
           from uk_pre_customer_spine_offer_is.td_offer_discount_p8 td
           left outer join uk_tds_refdata_eod_is.cc_refdata_bsbcatalogueproductprice price
             on price.catalogueproductid = td.discounted_product_id
            and price.currencycode = td.currency
            and td.effective_from_dt >= price.startdate
            and td.effective_from_dt <  ifnull(price.enddate,timestamp('2999-12-31 23:59:59 UTC'))
            and ifnull(price.rdmdeletedflag,'N') = 'N'
            and price.rdmaction != 'D'
           left outer join uk_tds_refdata_eod_is.cc_refdata_bsbcatalogueproductprice price2
             on price2.catalogueproductid = td.discounted_product_id
            and price2.currencycode = td.currency
            and td.activation_date >= price2.startdate
            and td.activation_date <  ifnull(price2.enddate,timestamp('2999-12-31 23:59:59 UTC'))
            and ifnull(price2.rdmdeletedflag,'N') = 'N'
            and price2.rdmaction != 'D'
           left join uk_tds_refdata_eod_is.cc_refdata_bsboffervaluediscount ovd
             on td.discount_id = ovd.offerdiscountid
            and td.effective_from_dt >= ifnull(ovd.effectivefromdate,timestamp('1901-01-01 00:00:00 UTC'))
            and td.effective_from_dt < ifnull(ovd.effectivetodate,timestamp('2999-12-31 23:59:59 UTC'))
          where (ifnull(td.activation_date,ifnull(td.termination_date,timestamp('2999-12-31 23:59:59 UTC'))) <= effective_from_dt
                and effective_from_dt <= ifnull(td.termination_date,timestamp('2999-12-31 23:59:59 UTC')))
 )cdc
 where ifnull(previous_current_price,0)   != discounted_product_current_price
    or ifnull(previous_status,'?')        != status_code
;
             
------------------------------------------------------------------------------------------------------------
--Offer values are not always able to be identified by simply joining to the offer discount tables, 
-- due to bsborderline not always containing a discountid.  In these scenarios some additional transformation 
-- is required to identify the offer value, this is completed by matching the offer description to a set of patterns.
-- REGEX expressions have all been setup with the values put into STRINGS to avoid breaking VM' chr(124) is a pipe character '|'
-- chr(92) is a backslash character '\' Each STRING will be put into a REGEX command to determine whether its 
--PRICE PROTECTION,PERCENT or VALUE
-------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p10:WRITE_TRUNCATE:     
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
       concat('voucher',chr(124),'mastercard') as regex_voucher_master_card,
       concat('free',chr(124),'pause',chr(124),'no',chr(92),'s*charge') as regex_free_pause_nocharge,
       concat('(',chr(92),'d+)',chr(92),'s*%') as regex_any_percentage,
       concat('half price',chr(124),'1/2',chr(92),'s*price') as regex_halfprice,
       concat('(?:discount',chr(92),'s*of)',chr(92),'s*(?:(?:£',chr(124),'gbp',chr(124),'eur(?:o)?',chr(124)) as d1,
       concat('€)',chr(92),'s*',chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(124),chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(92),'s*(?:£',chr(124),'gbp',chr(124),'eur(?:o)?') as d2,
       concat(chr(124),'€))',chr(124),'(?:(?:£',chr(124),'gbp',chr(124),'eur(?:o)?',chr(124)) as d3,
       concat('€)',chr(92),'s*',chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(124),chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(92),'s*(?:£',chr(124),'gbp',chr(124),'eur(?:o)?') as d4,
       concat(chr(124),'€))',chr(92),'s*(?:off)') as d5,
       concat('(?:for)',chr(92),'s*(?:(?:£',chr(124),'gbp',chr(124)) as f1,
       concat('eur(?:o)?',chr(124),'€)',chr(92),'s*',chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(124),chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(92),'s*(?:£',chr(124),'gbp') as f2,
       concat(chr(124),'eur(?:o)?',chr(124),'€))',chr(124),'(?:at)?',chr(92),'s*(?:(?:£',chr(124),'gbp',chr(124)) as f3,
       concat('eur(?:o)?',chr(124),'€)',chr(92),'s*',chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(124),chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(92),'s*(?:£',chr(124),'gbp') as f4,
       concat(chr(124),'eur(?:o)?',chr(124),'€))',chr(92),'s*(?:per',chr(92),'s*month',chr(124),'for)',chr(124)) as f5,
       concat('(?:discounted',chr(92),'s*to)',chr(92),'s*(?:(?:£',chr(124),'gbp',chr(124),'eur(?:o)?',chr(124)) as f6,
       concat('€)',chr(92),'s*',chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(124),chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(92),'s*(?:£',chr(124),'gbp',chr(124),'eur(?:o)?') as f7,
       concat(chr(124),'€))',chr(124),'(?:(?:£',chr(124),'gbp',chr(124),'eur(?:o)?',chr(124)) as f8,
       concat('€)',chr(92),'s*',chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(124),chr(92),'d+(?:',chr(92),'.',chr(92),'d+)?',chr(92),'s*(?:£',chr(124),'gbp',chr(124),'eur(?:o)?') as f9,
       concat(chr(124),'€))') as f10,
       case 
         when lower(offer.description) like '%price protection%' then
           'PRICE PROTECTION' 
         when ofpd.discountpercentage is not null then 
           'PERCENT'
         when ofvd.value is not null then 
           'VALUE'
         else 
           null
       end discount_type,
       case 
          when lower(offer.description) like '%price protection%' then
            0
         when ofpd.discountpercentage is not null then 
           ofpd.discountpercentage
         when ofvd.value is not null then 
           ofvd.value
         else
           null
       end discount,   
       td.discounted_product_event_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.source_field,
       td.active_flag,
       td.status_code,
       td.original_portfolio_offer_id,
       td.offerid,
       td.discounted_product_starting_price,
       td.discounted_product_current_price,
       td.discount_id,
       td.currency,
       td.discount_version,
       td.value,
       offer.is_gross,
       case
         when offer.is_gross is null
           or offer.is_gross = 1 then
           true
         when offer.is_gross = 0 then
           false
       end                                                 as gross_flag,
       offer.discountpriority                              as discount_priority,
       offer.description,
       offer.offer_type                                    as transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_p9 td
  inner join uk_tds_refdata_eod_is.cc_refdata_bsboffer offer
    on td.offerid = offer.id
   and offer.rdmdeletedflag = 'N'
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsbofferpercentagediscount ofpd
   on td.discount_id = ofpd.bsbofferdiscountid
  and td.created_dt >= ifnull(ofpd.effectivefromdate,timestamp('1901-01-01 00:00:00.0 UTC'))
  and td.created_dt < ifnull(ofpd.effectivetodate,timestamp('2999-12-31 23:59:59 UTC'))
  and ofpd.rdmdeletedflag = 'N'
  left outer join uk_tds_refdata_eod_is.cc_refdata_bsboffervaluediscount ofvd
   on td.discount_id = ofvd.offerdiscountid
  and td.currency    = ofvd.currencycode
  and ofvd.rdmdeletedflag = 'N'
  and td.created_dt  >= ifnull(ofvd.effectivefromdate,timestamp('1901-01-01 00:00:00.0 UTC'))
  and td.created_dt < ifnull(ofvd.effectivetodate,timestamp('2999-12-31 23:59:59 UTC'));
 
------------------------------------------------------------------------------------------------------------
-- When bsborderline doesnt containing a discountid, offer description is matched to to a set of patterns.
-- The STRING values created above are now slotted into each of the REGEX commands
-- again this is to calculate whether the discount is a PRICE PROTECTION,PERCENT and VALUE
-- The REGEX_REPLACE is grabbing an INTEGER value.
------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p11:WRITE_TRUNCATE:     
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
       concat(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10) as max_value_flag_regex_string,
       case 
          when discount_type is null
          and regexp_extract(lower(td.description),regex_voucher_master_card) is not null then
           null
          when discount_type is null
          and regexp_extract(lower(td.description),regex_free_pause_nocharge) is not null then
           'PERCENT'            
          when discount_type is null 
          and regexp_extract(lower(td.description),regex_halfprice) is not null then 
           'PERCENT'
          when discount_type is null 
          and regexp_extract(lower(td.description),regex_any_percentage) is not null then 
           'PERCENT'
         when discount_type is null
         and regexp_replace(regexp_extract(lower(td.description),concat(d1,d2,d3,d4,d5)),r'[^\\\d\\\.]','') is not null then
           'VALUE'
        else 
          discount_type
       end discount_type,
       case
         when discount is null 
          and regexp_extract(lower(td.description),regex_voucher_master_card)  is not null then 
           null
         when discount is null
          and regexp_extract(lower(td.description),regex_free_pause_nocharge) is not null then 
           100
         when discount is null 
          and regexp_extract(lower(td.description),regex_halfprice) is not null then 
           50
         when discount is null 
          and regexp_extract(lower(td.description),regex_any_percentage) is not null then 
           cast(regexp_extract(lower(td.description),regex_any_percentage) as numeric)
        when discount_type is null
          and cast(regexp_replace(regexp_extract(lower(td.description),concat(d1,d2,d3,d4,d5)),r'[^\\\d\\\.]','') as numeric) is not null then
              cast(regexp_replace(regexp_extract(lower(td.description),concat(d1,d2,d3,d4,d5)),r'[^\\\d\\\.]','') as numeric)
         else
           discount
       end discount,
       td.discounted_product_event_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.source_field,
       td.active_flag,
       td.status_code,
       td.original_portfolio_offer_id,
       td.gross_flag,
       td.discount_priority,
       td.offerid,
       td.discounted_product_starting_price,
       td.discounted_product_current_price,
       td.discount_id,
       td.currency,
       td.discount_version,
       td.value,
       td.description,
       td.transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_p10 td;  
   
------------------------------------------------------------------------------------------------------------
-- The max_value_flag is now created on any records that still have a NULL discount and discount_type
-- If a value is brought back, it is later used in bringing a QUOTE value.
-- The integer value is then grabbed out of the REGEX expression for this section.
------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p12:WRITE_TRUNCATE:     
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
       case
         when (   discount is null
               or discount_type is null)
          and regexp_replace(regexp_extract(lower(td.description),max_value_flag_regex_string),r'[^\\\d\\\.]','') is not null then
           true
         else
           false
       end max_value_flag,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       source_field,
       active_flag,
       status_code,
       original_portfolio_offer_id,
       offerid,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       td.discount_version,
       td.value,
       description,
       transaction_type
  from uk_pre_customer_spine_offer_is.td_offer_discount_p11 td; 
  
------------------------------------------------------------------------------------------------------------
-- Using the max_value_flag whilst TRUE it can then be deemed as a VALUE.
-- The discount then will use the quotedprice in this scenario instead of the REGEX'd value.
-- This is the final section of the REGEX calculations and will soon prepare the data for stacking.
------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p13:WRITE_TRUNCATE:     
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
       case
         when discount_type is null and max_value_flag then
         'VALUE'
         else
       discount_type
       end discount_type ,
       case
         when discount is null and max_value_flag then
           abs(pu_index.quotedprice)
         else
           discount
       end discount,
       td.max_value_flag,
       td.discounted_product_event_id,
       td.discounted_priceable_unit_id,
       td.discounted_product_id,
       td.source_field,
       td.active_flag,
       td.status_code,
       td.original_portfolio_offer_id,
       td.offerid,
       td.gross_flag,
       td.discount_priority,
       td.discounted_product_starting_price,
       td.discounted_product_current_price,
       td.discount_id,
       td.currency,
       td.discount_version,
       td.value,
       td.description,
       td.transaction_type,
       pu_index.quotedprice
  from uk_pre_customer_spine_offer_is.td_offer_discount_p12 td
  left outer join ( select portfolioofferid,
                           quotedprice,
                           row_number() over (partition by portfolioofferid
                                                  order by created,
                                                           commit_ts,
                                                           effectivedate,
                                                           id) as rn
                      from uk_tds_chordiant_eod_is.cc_chordiant_bsbpriceableunit pu
                     where portfolioofferid is not null) pu_index
    on td.portfolio_offer_id = pu_index.portfolioofferid
   and pu_index.rn = 1;  

------------------------------------------------------------------------------------------------------------
-- Filtering based on discount !=0 and discount_type <> 'PRICE PROTECTION'
------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p13_1:WRITE_TRUNCATE:     
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
       original_portfolio_offer_id,
       offerid,
       gross_flag,
       discount_priority,
       discounted_product_starting_price,
       discounted_product_current_price,
       discount_id,
       currency,
       discount_version,
       value,
       description,
       transaction_type,
       quotedprice
  from uk_pre_customer_spine_offer_is.td_offer_discount_p13
 where ifnull(discount,0) != 0
    or discount_type = 'PRICE PROTECTION';

------------------------------------------------------------------------------------------------------------
-- Precurssor calculations before offer stacking starts.
------------------------------------------------------------------------------------------------------------  
uk_pre_customer_spine_offer_is.td_offer_discount_p14:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
       description,
       transaction_type,
       quotedprice
  from uk_pre_customer_spine_offer_is.td_offer_discount_p13_1 td;

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
uk_pre_customer_spine_offer_is.td_offer_discount_p14_1:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
 from  uk_pre_customer_spine_offer_is.td_offer_discount_p14;

-----------------------------------------------------------------------------------------------------
-- This is the second stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
-----------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p14_2:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
  from uk_pre_customer_spine_offer_is.td_offer_discount_p14_1;

-----------------------------------------------------------------------------------------------
-- This is the third stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
-----------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p14_3:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
  from uk_pre_customer_spine_offer_is.td_offer_discount_p14_2;

-----------------------------------------------------------------------------------------------------------------------------
-- This is the forth stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
-----------------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p14_4:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
  from uk_pre_customer_spine_offer_is.td_offer_discount_p14_3;

-------------------------------------------------------------------------------------------------------------------------------
-- This is the fifth stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
--------------------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p14_5:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
  from uk_pre_customer_spine_offer_is.td_offer_discount_p14_4;


-----------------------------------------------------------------------------------
-- This is the sixth stack calculation using the columns above.
-- By lagging to previous record that is calculated we are building up the data
-- based on OFFER_STACK_RN, STACK_OFFER_SUM and PRICE_PROTECTION_STACK_SUM
---------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount_p14_6:WRITE_TRUNCATE:
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
       original_portfolio_offer_id,
       offerid,
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
  from uk_pre_customer_spine_offer_is.td_offer_discount_p14_5;

---------------------------------------------------------------------------------------------------------------------------
-- FINAL td table lagging effective_from_dts to create the final arrangement for EFFECTIVE_TO_DT
---------------------------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_discount:WRITE_TRUNCATE:
select portfolio_offer_id,
       effective_from_dt,
       lag(effective_from_dt,1,timestamp('2999-12-31 23:59:59 UTC')) over (partition by portfolio_offer_id
                                                                               order by effective_from_dt         desc,
                                                                                        effective_from_dt_csn_seq desc,
                                                                                        effective_from_dt_seq     desc) as effective_to_dt,
       created_dt,
       created_by_id,
       last_modified_dt,
       last_modified_by_id,
       original_portfolio_offer_id,
       portfolio_offer_priceable_unit_id,
       discounted_product_event_id,
       discounted_priceable_unit_id,
       discounted_product_id,
       discounted_product_current_price as discounted_product_price,
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
  from uk_pre_customer_spine_offer_is.td_offer_discount_p14_6;


