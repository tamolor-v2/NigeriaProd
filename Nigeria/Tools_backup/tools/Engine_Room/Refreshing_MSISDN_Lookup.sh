#!bin/bash
#Engine_Room
#Nabil

DATE=$(date -d '-1 day' '+%Y%m%d')

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into flare_8.msisdns_lookup select cast(a.msisdn_key as varchar) , to_hex(md5(to_utf8((cast(coalesce (a.msisdn_key,0) as varchar ))))) from flare_8.sdp_dmp_ma a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select distinct(cast(a.msisdn_key as varchar))  , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar ))))))
from flare_8.wbs_pm_rated_cdrs a left outer join flare_8.msisdns_lookup b
on cast(a.msisdn_key as varchar) = b.msisdn_key
where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.ewp_account_holders_dump a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(a.recruiter_msisdn) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.recruiter_msisdn,'0') as varchar )))))) from flare_8.ewp_account_holders_dump a left outer join flare_8.msisdns_lookup b on a.recruiter_msisdn = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.ers_vend_new a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(a.sendermsisdn) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.sendermsisdn,'0') as varchar )))))) from flare_8.ers_vend_new a left outer join flare_8.msisdns_lookup b on a.sendermsisdn = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.cs5_air_refill_ma a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.cs5_ccn_gprs_ma a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(a.calledcallingnumber) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.calledcallingnumber,'0') as varchar )))))) from flare_8.cs5_ccn_voice_ma a left outer join flare_8.msisdns_lookup b on a.calledcallingnumber = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.cs5_ccn_voice_ma a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.cs5_ccn_sms_ma a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(a.calledcallingnumber) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.calledcallingnumber,'0') as varchar )))))) from flare_8.cs5_ccn_sms_ma a left outer join flare_8.msisdns_lookup b on a.calledcallingnumber = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)), (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.Balances a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(a.accountno) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.accountno,'0') as varchar )))))) from flare_8.Balances a left outer join flare_8.msisdns_lookup b on a.accountno = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;

insert into flare_8.msisdns_lookup
select  distinct(cast(a.msisdn_key as varchar)) , (to_hex(md5(to_utf8(distinct(cast(coalesce (a.msisdn_key,0) as varchar )))))) from flare_8.cb_newreg_bioupdt_pool_daily a left outer join flare_8.msisdns_lookup b on cast(a.msisdn_key as varchar) = b.msisdn_key where b.msisdn_key is null and a.tbl_dt=$DATE;" 
