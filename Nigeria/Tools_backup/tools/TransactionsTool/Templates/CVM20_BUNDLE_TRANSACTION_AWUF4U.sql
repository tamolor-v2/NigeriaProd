start transaction;
delete from cvm_db.cvm20_bundle_transaction_awuf4u where tbl_dt =yyyymmddRunDate; 
insert into cvm_db.cvm20_bundle_transaction_awuf4u 
( date_key,msisdn_key,amount,hr  ,rec_count,event_type,tbl_dt  )
SELECT
  date_key
, msisdn_key
, amount
, hr
, rec_count
, event_type
,cast (date_key  as int ) tbl_dt 
FROM
  nigeria.daas_daily_usage_by_msisdn aa
WHERE ( date_key = yyyymmddRunDate) 
AND (product_type IN ('AWUF_4_YOU'))
AND (amount > 0) ; 
commit;
