--------------------------------------------------------------------------------
--
-- filename      : spine_offer_td_offer_status.sql
-- author        : Ashutosh Ranjan
-- date created  : 28th July 2021
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
-- 280721  ARJ04   Spine  Initial Version                                    1.0
-- 030921  ARJ04   Spine  transformation logic added 
--                        for effective_from_dt column BDP-19183             1.1
-- 031121  SGW01   SOIP   Added missing alias (issue BDP-19506)              1.2
--------------------------------------------------------------------------------
-- Get sub-set of data from driving table FC_CHORDIANT_BSBPORTFOLIOOFFER
-- Join to CC_CHORDIANT_PICKLIST for data enrichment
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_p1:WRITE_TRUNCATE:
select po.id                                     portfolio_offer_id,
       case   
         when po.statuschangeddate is not null then  
              po.statuschangeddate
       else   po.effective_from_dt
       end                                       effective_from_dt,
       po.effective_from_dt_csn_seq              effective_from_dt_csn_seq,
       po.effective_from_dt_seq                  effective_from_dt_seq,
       po.statuschangeddate                      effective_to_dt,
       po.status                                 status_code,
       p.codedesc                                status,
       po.statusreasoncode                       reason_code,
       p2.codedesc                               reason
  from uk_tds_chordiant_eod_fc_is.fc_chordiant_bsbportfoliooffer po
  left join uk_tds_chordiant_eod_is.cc_chordiant_picklist p
    on (    po.status = p.code
        and upper(p.codegroup) in ('PORTFOLIOOFFERSTATUS')
        and p.logically_deleted = 0
        and p.rdmdeletedflag = 'N')        
  left join uk_tds_chordiant_eod_is.cc_chordiant_picklist p2
    on (    po.statusreasoncode = p2.code
        and upper(p2.codegroup) in ('PORTFOLIOOFFEREVENT')
        and p2.logically_deleted = 0
        and p2.rdmdeletedflag = 'N')
 where po.logically_deleted = 0;

--------------------------------------------------------------------------------
-- Write_truncate p2 table.
-- Lagging on all type 2 data changing columns for direct
-- comparison, to make identifying changes easier
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status_p2:WRITE_TRUNCATE:
select to_base64(sha512(concat(p1.portfolio_offer_id, ':',
                               p1.effective_from_dt, ':',
                               p1.effective_from_dt_csn_seq, ':',
                               p1.effective_from_dt_seq
                               )
                        )
                )           uniq_hash, 
       p1.portfolio_offer_id,
       p1.effective_from_dt,
       p1.effective_from_dt_csn_seq,
       p1.effective_from_dt_seq,
       p1.effective_to_dt,    
       p1.status_code,
       lag(p1.status_code,1) over (partition by p1.portfolio_offer_id
                                       order by p1.effective_from_dt,
                                                p1.effective_from_dt_csn_seq,
                                                p1.effective_from_dt_seq) previous_status_code,
       p1.status,
       p1.reason_code,
       lag(p1.reason_code,1) over (partition by p1.portfolio_offer_id
                                       order by p1.effective_from_dt,
                                                p1.effective_from_dt_csn_seq,
                                                p1.effective_from_dt_seq) previous_reason_code,
       p1.reason
  from uk_pre_customer_spine_offer_is.td_offer_status_p1 p1;

----------------------------------------------------------------------------------------------------------
-- Write_truncate final td table.
-- Filter out all records where no change in type
-- 2 columns as per doc will be checked for type changes and doing uion all with SOIP tables
----------------------------------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status:WRITE_TRUNCATE:
select uniq_hash, 
       p2.portfolio_offer_id,
       p2.effective_from_dt,
       p2.effective_from_dt_csn_seq,
       p2.effective_from_dt_seq,
       cast(null as timestamp) effective_to_dt,
       p2.status_code,
       p2.status,
       p2.reason_code,
       p2.reason
  from uk_pre_customer_spine_offer_is.td_offer_status_p2 p2
 where ifnull(p2.status_code, '?') != ifnull(p2.previous_status_code, '?')
    or ifnull(p2.reason_code, '?') != ifnull(p2.previous_reason_code, '?');

--------------------------------------------------------------------------------
-- Update final td. Set effective_to_dt column using effective_from_dt
-- or the high date for open records
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_status:UPDATE:
update uk_pre_customer_spine_offer_is.td_offer_status trg
   set trg.effective_to_dt = src.effective_to_dt
  from (select uniq_hash,
           lag(effective_from_dt,
            1,
            timestamp('2999-12-31 23:59:59')
            ) over (partition by portfolio_offer_id 
                        order by effective_from_dt             desc,
                                 effective_from_dt_csn_seq     desc,
                                 effective_from_dt_seq         desc) effective_to_dt
          from uk_pre_customer_spine_offer_is.td_offer_status) src
 where trg.uniq_hash = src.uniq_hash;

 