start transaction;
delete from CVM_DB.CVM20_BUNDLE_TRANSACTION where tbl_dt=yyyymmddRunDate;
insert into CVM_DB.CVM20_BUNDLE_TRANSACTION
SELECT
msisdn_key 
,transaction_date_time
,channel_name 
,product_id 
,product_name 
,product_type 
,product_subtype
,renewal_adhoc
,original_timestamp_enrich
,action 
,expiry_time
,grace_period 
,offer_id 
,charging_amount
,transaction_charges
,auto_renewal_consent 
,tbl_dt 
FROM 
flare_8.cis_cdr c 
where tbl_dt=yyyymmddRunDate;
commit;
