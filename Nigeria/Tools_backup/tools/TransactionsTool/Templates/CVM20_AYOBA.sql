start transaction;
delete from CVM_DB.CVM20_AYOBA where tbl_dt=yyyymmddRunDate;
insert into cvm_db.cvm20_ayoba
SELECT 
msisdn_key 
,event_timestamp_enrich
,unique_event_id 
,event_name
,sign_up_method
,tbl_dt
FROM
flare_8.ayobaevents
where tbl_dt=yyyymmddRunDate;
commit;
