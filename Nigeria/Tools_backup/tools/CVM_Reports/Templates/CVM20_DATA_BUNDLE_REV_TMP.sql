start transaction;
delete from cvm_db.cvm20_data_bundle_rev_tmp where tbl_dt=yyyymmddRunDate;
insert into cvm_db.CVM20_DATA_BUNDLE_REV_TMP
select 
msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')   monthid ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID,  
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
, sum(data_bundle_rev) data_bundle_rev 
, sum(combo_rev) total_rev_combo_bundle 
, sum(voice_bundle) total_rev_voice_bundle
, sum( coalesce(data_bundle_rev,0)+coalesce(data_bundle_rev,0)+coalesce(data_bundle_rev,0)) total_rev_bundle
, cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int)
from   
( select tbl_dt,date_key,msisdn_key,sub_fee data_bundle_rev, 0 combo_rev ,0 voice_bundle , product_name,channel,day , 'DATA' bundle_type
from nigeria.vng_prepaid_data_bundle 
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
union all   
select tbl_dt,date_key,msisdn_key,sub_fee data_bundle_rev,0 combo_rev,  0 voice_bundle , product_name,channel,day , 'SOCIAL' bundle_type
from nigeria.vng_daily_social_bundle  
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
union all   
select tbl_dt,date_key,msisdn_key,sub_fee data_bundle_rev, 0 combo_rev,  0 voice_bundle ,product_name,channel,day  , 'DATA' bundle_type
from nigeria.vng_postpaid_data_bundle
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
union all 
select tbl_dt,date_key,msisdn_key,0 data_bundle_rev, sub_fee combo_rev,0 voice_bundle ,product_name,channel,day , 'COMBO' bundle_type
from nigeria.vng_xtral_value_bundle  
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
union all  
select tbl_dt,date_key,msisdn_key,0 data_bundle_rev, 0 combo_rev,sub_fee voice_bundle ,product_name,channel,day  , 'VOICE' bundle_type
from nigeria.vng_postpaid_voice_bundle 
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
union all
select tbl_dt,date_key,msisdn_key,0 data_bundle_rev, 0 combo_rev,sub_fee voice_bundle,product_name,channel,day  , 'VOICE' bundle_type
from nigeria.vng_roaming_bundle 
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek 
)  
where date_key  between yyyymmddRunDate and yyyyRunDateWeek
group by   msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y'),
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')  ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v')  , 
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)  ,                 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint),
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) ;
commit;
