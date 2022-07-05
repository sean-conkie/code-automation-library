--------------------------------------------------------------------------------
--
-- Filename      : dim_offer_type.sql
-- Author        : Cloud Composer
-- Date Created  : 05 Jul 2022
--
--------------------------------------------------------------------------------
--
-- Description   : SQL file auto-generated for dim_offer_type
--
-- Comments      : NA
--
-- Usage         : Standard BQSQL Call
--
-- Called By     : dim_offer_type
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
--------------------------------------------------------------------------------

truncate table uk_pub_customer_spine_offer_is.dim_offer_type; 
insert into uk_pub_customer_spine_offer_is.dim_offer_type (id,
       offer_detail_id,
       offer_type_id,
       created_by_id,
       created_dt,
       last_modified_dt,
       last_modified_by_id,
       code,
       description) 
select a.id,
       a.offerid                                           offer_detail_id,
       a.offertypeid                                       offer_type_id,
       a.createdby                                         created_by_id,
       a.created                                           created_dt,
       a.lastupate                                         last_modified_dt,
       a.updatedby                                         last_modified_by_id,
       b.code,
       b.description
  from uk_tds_refdata_eod_is.cc_refdata_bsboffertooffertype a
  left join uk_tds_refdata_eod_is.cc_refdata_bsboffertype b
    on (    a.offertypeid = b.id
        and b.rdmaction   != 'D')
 where a.rdmaction != 'D'
;
