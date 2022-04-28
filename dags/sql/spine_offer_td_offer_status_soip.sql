--------------------------------------------------------------------------------
--
-- filename      : spine_offer_td_offer_status_soip.sql
-- author        : Ashutosh Ranjan
-- date created  : 02th Aug 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate transient table's required to populate
--                 dimension table dim_offer_status
--
-- Comments      : NA
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : spine_offer_dim_1.sh
--
-- Calls         : none
--
-- Parameters    : 1) lower_date_bound - DD-MON-YYYY HH24:MI:SS
--                 2) upper_date_bound - DD-MON-YYYY HH24:MI:SS.SSSSS
--
-- Exit codes    : 0 - Success
--                 1 - Failure
--
-- Revisions
-- =============================================================================
-- Date    Userid  MR#         Comments                                     Ver.
-- ------  ------  ----------  -------------------------------------------  ----
-- 280721  ARJ04   cust-spine  Initial Version                              1.0
-- 261021  SGW01   SoIP        Rewrite to use fc_chordiant_offer            1.1
--------------------------------------------------------------------------------
-- Getting SOIP Data
-- Get sub-set of data from driving table FC_CHORDIANT_OFFER
-- and lag for isPendingCancel flag
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_soip_p1:WRITE_TRUNCATE:
select id                           portfolio_offer_id,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       eventcode reason_code,
       effective_from_dt            source_effective_from_dt,
       rcdiscountstartdate,
       rcdiscountenddate,
       created,
       ifnull(ispendingcancel, 0)   ispendingcancel,
       ifnull(lag(ispendingcancel, 1) over (partition by id order by effective_from_dt,
                                                                     effective_from_dt_csn_seq,
                                                                     effective_from_dt_seq)
              ,0)                   previous_ispendingcancel
  from uk_tds_chordiant_eod_fc_is.fc_chordiant_offer
 where logically_deleted = 0;

--------------------------------------------------------------------------------
-- Write_truncate p2 table.
-- Calculate effective_from_dt and status_code
-- using logic as laid out in the confluence spec
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_soip_p2:WRITE_TRUNCATE:
select portfolio_offer_id,
       created   effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,    
       'PND'     status_code,
       'Pending' status,
       reason_code
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p1
  where (   rcdiscountstartdate is null
         or rcdiscountstartdate > source_effective_from_dt)
         and created < ifnull(rcdiscountstartdate, timestamp('2999-12-31 23:59:59'))
union all 
select portfolio_offer_id,
       case
         when created < rcdiscountstartdate then
           rcdiscountstartdate
         when cast(source_effective_from_dt as date) = cast(rcdiscountstartdate as date) then
           source_effective_from_dt
         when cast(created as date) = cast(rcdiscountstartdate as date) then
           created
         else
           rcdiscountstartdate
       end           effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       'ACT'         status_code,
       'Active'      status,
       reason_code
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p1
  where (         rcdiscountstartdate is not null
         and (   rcdiscountenddate is null
              or rcdiscountenddate > source_effective_from_dt)
         and          ispendingcancel = 0
         and previous_ispendingcancel = 0)
union all 
select portfolio_offer_id,
       source_effective_from_dt effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,     
       'PTM'                      status_code,
       'Pending Terminated'       status,
       reason_code
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p1
  where (rcdiscountstartdate is not null
         and (   rcdiscountenddate is null
              or rcdiscountenddate > source_effective_from_dt)
         and ispendingcancel = 1)
union all 
select portfolio_offer_id,
       source_effective_from_dt  effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       'ACT'                       status_code,
       'Active'                    status,
       reason_code
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p1
  where (         rcdiscountstartdate is not null
         and (   rcdiscountenddate is null
              or rcdiscountenddate > source_effective_from_dt)
         and          ispendingcancel = 0
         and previous_ispendingcancel = 1)
union all 
select portfolio_offer_id,
       case
         when rcdiscountenddate > source_effective_from_dt then
           rcdiscountenddate
         else
           source_effective_from_dt
       end           effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       'TMD'             status_code,
       'Terminated'      status,
       reason_code
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p1
  where (    rcdiscountenddate is not null
         and rcdiscountenddate <= current_timestamp);

--------------------------------------------------------------------------------
-- Write_truncate p3 table.
-- Derive previous_status_code and previous_reason_code
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_soip_p3:WRITE_TRUNCATE:
select to_base64(sha512(concat(portfolio_offer_id, ':',
                               status_code, ':',
                               ifnull(reason_code,'?'), ':',
                               effective_from_dt, ':',
                               effective_from_dt_csn_seq, ':',
                               effective_from_dt_seq
                              )
                       )
                 )                                                  uniq_hash,
       portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       status_code,
       lag(status_code,1) over (partition by portfolio_offer_id
                                    order by effective_from_dt,
                                             effective_from_dt_csn_seq,
                                             effective_from_dt_seq) previous_status_code,
       status,
       reason_code,
       lag(reason_code,1) over (partition by portfolio_offer_id
                                    order by effective_from_dt,
                                             effective_from_dt_csn_seq,
                                             effective_from_dt_seq) previous_reason_code
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p2;

----------------------------------------------------------------------------------------------------------
-- Write_truncate final td table.
-- Filter out all records where no change in type
-- 2 columns as per doc will be checked for type changes
----------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_soip:WRITE_TRUNCATE:
select uniq_hash,
       portfolio_offer_id,
       effective_from_dt,
       effective_from_dt_csn_seq,
       effective_from_dt_seq,
       cast(null as timestamp) effective_to_dt,
       status_code,
       status,
       reason_code,
       cast(null as string)    reason
  from uk_pre_customer_spine_offer_is.td_offer_status_soip_p3
 where (   ifnull(status_code, '?') != ifnull(previous_status_code, '?')
        or ifnull(reason_code, '?') != ifnull(previous_reason_code, '?'));

--------------------------------------------------------------------------------
-- Update final td. Set effective_to_dt column using effective_from_dt
-- or the high date for open records
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_soip:UPDATE:
update uk_pre_customer_spine_offer_is.td_offer_status_soip trg
   set trg.effective_to_dt = src.effective_to_dt
  from (select uniq_hash,
               lag(effective_from_dt,
                   1,
                   timestamp('2999-12-31 23:59:59')
                   ) over (partition by portfolio_offer_id
                               order by effective_from_dt         desc,
                                        effective_from_dt_csn_seq desc,
                                        effective_from_dt_seq     desc) effective_to_dt
          from uk_pre_customer_spine_offer_is.td_offer_status_soip) src
 where trg.uniq_hash = src.uniq_hash;
