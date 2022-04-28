--------------------------------------------------------------------------------
--
-- Filename      : spine_td_offer_type.sql
-- Author        : Sandeep Bibekar
-- Date Created  : 19th Jul 2021
--
--------------------------------------------------------------------------------
--
-- Description   : Populate TD_OFFER_TYPE as a precursor to populating
--                                 DIM_OFFER_TYPE
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
-- Revisions
-- =============================================================================
-- Date     userid  MR#       Comments                                      Ver.
-- ------   ------  ------    --------------------------------------------  ----
-- 190721   sbr55   Spine     initial version                               1.0
-- 260721   abr87   Spine     updated to coding standards                   1.1
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Insert data into transient table
--------------------------------------------------------------------------------
uk_pre_customer_spine_offer_is.td_offer_type:WRITE_TRUNCATE:
select oto.id,
       oto.offerid      offer_detail_id,
       oto.offertypeid  offer_type_id,
       oto.created      created_dt,
       oto.createdby    created_by_id,
       oto.lastupdate   last_modified_dt,
       oto.updatedby    last_modified_by_id,
       bot.code,
       bot.description, 
  from uk_tds_refdata_eod_is.cc_refdata_bsboffertype bot     
 inner join uk_tds_refdata_eod_is.cc_refdata_bsboffertooffertype oto    
    on (    bot.id         = oto.offertypeid   
        and oto.rdmaction != 'D')
 where bot.rdmaction != 'D';
 