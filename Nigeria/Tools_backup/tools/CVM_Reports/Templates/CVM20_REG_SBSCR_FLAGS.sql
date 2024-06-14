start transaction;
insert into cvm_db.CVM20_REG_SBSCR_FLAGS
with refl_and_bundle as 
( select tbl_dt,date_key, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int ) weekid, try_cast(substring(CAST(tbl_dt AS varchar(10)), 7,8) as int) dayid, msisdn_key,sub_fee totalcharge_money,product_name,channel, upper(substring(day,1,3)) dow, bundle_type from ( select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'DATA' bundle_type from nigeria.vng_prepaid_data_bundle union all select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'SOCIAL' bundle_type from nigeria.vng_daily_social_bundle union all select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'COMBO' bundle_type from nigeria.vng_xtral_value_bundle union all select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'DATA' bundle_type from nigeria.vng_postpaid_data_bundle union all select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'VOICE' bundle_type from nigeria.vng_postpaid_voice_bundle union all select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'VOICE' bundle_type from nigeria.vng_roaming_bundle ) where try_cast(substring(CAST(tbl_dt AS varchar(10)), 1,4) as int) = cast(substring(cast(yyyymmddRunDate as varchar),1,4) as int) ) ,
sbsc_Reg_flags as 
( select try_cast(msisdn_key as bigint) msisdn_key ,tbl_dt, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) weekid, 'NULL' status, 'MOD' channel from nigeria.modtransactions a where (try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) = try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int)) and try_cast(substring(CAST(tbl_dt AS varchar(10)), 1,4) as int) = cast(substring(CAST(yyyymmddRunDate AS varchar(10)),1,4) as int) union all select try_cast(msisdn as bigint) msisdn, tbl_dt, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) weekid, case when upper(status)= 'EXISTING' then 'Subscription' else 'Registration' end status, 'MYMTNAPP' channel from nigeria.smartapp_user b where (try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) = try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int)) and try_cast(substring(CAST(tbl_dt AS varchar(10)), 1,4) as int) = cast(substring(CAST(yyyymmddRunDate AS varchar(10)),1,4) as int) union all select msisdn_key, date_key, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') as int) weekid, 'Subscrition' status , 'MOD' channel from nigeria.daas_daily_usage_by_msisdn where product_type ='RECHARGES' and event_type like '%MOD%' and (try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') as int) = try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int)) and try_cast(substring(CAST(date_key AS varchar(10)), 1,4) as int) = cast(substring(CAST(yyyymmddRunDate AS varchar(10)),1,4) as int) union all select msisdn_key,tbl_dt, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) weekid, case when upper(event_name) = 'SIGNUP' then 'Registration' else 'Subscription' end status , 'AYOBA' channel from flare_8.ayobaevents where (try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) = try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int)) and try_cast(substring(CAST(tbl_dt AS varchar(10)), 1,4) as int) = cast(substring(CAST(yyyymmddRunDate AS varchar(10)),1,4) as int) union all select distinct Msisdn_key, tbl_dt, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) weekid, 'PAID' status, 'MUSICTIME' channel from nigeria.hsdp_sumd where upper(product_name) like '%MUSIC%TIME%' and (try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') as int) = try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int)) and try_cast(substring(CAST(tbl_dt AS varchar(10)), 1,4) as int) = cast(substring(cast(yyyymmddRunDate as varchar),1,4) as int) ) 
select coalesce(a.msisdn_key, b.msisdn_key) msisdn_key ,
coalesce(a.weekid, b.weekid) weekid ,
max(sbsc_flag_mod) sbsc_flag_mod ,
max(reg_flag_mod) reg_flag_mod ,
max(reg_flag_ayoba) reg_flag_ayoba ,
max(sbsc_flag_musictime) sbsc_flag_musictime ,
max(reg_flag_musictime) reg_flag_musictime ,
max(sbsc_flag_ayoba) sbsc_flag_ayoba ,
max(sbsc_flag_mtnapp) sbsc_flag_mtnapp ,
max(reg_flag_mtnapp) reg_flag_mtnapp ,
max(reg_flag_apps_n_services) reg_flag_apps_n_services ,
max(sbsc_flag_apps_n_services) sbsc_flag_apps_n_services ,
max(vas_flag) vas_flag ,max(bundle_flag) bundle_flag,
yyyymmddRunDate as tbl_dt 
from (
(
select msisdn_key, try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int) weekid, (case when channel = 'MOD' and ((subsc_cnt1 <> 0 and subsc_cnt3 > 1) or subsc_cnt3 >= 1) then 1 end) sbsc_flag_mod, (case when channel = 'MOD' and ((subsc_cnt1 = 0 and subsc_cnt3 = 1) or subsc_cnt3 >= 1) then 1 end) reg_flag_mod, (case when channel = 'AYOBA' and ((subsc_cnt1 <> 0 and subsc_cnt3 > 1) or subsc_cnt3 >= 1) then 1 end) sbsc_flag_Ayoba, (case when channel = 'AYOBA' and ((subsc_cnt1 = 0 and subsc_cnt3 = 1) or subsc_cnt3 >= 1) then 1 end) reg_flag_Ayoba, (case when channel = 'MUSICTIME' and ((subsc_cnt1 <> 0 and subsc_cnt3 > 1) or subsc_cnt3 >= 1) then 1 end) sbsc_flag_musictime, (case when channel = 'MUSICTIME' and ((subsc_cnt1 = 0 and subsc_cnt3 = 1 ) or subsc_cnt3 >= 1) then 1 end) reg_flag_musictime, (case when channel = 'MYMTNAPP' and ((subsc_cnt1 <> 0 and subsc_cnt3 > 1) or subsc_cnt3 >= 1) then 1 end) sbsc_flag_mtnapp, (case when channel = 'MYMTNAPP' and ((subsc_cnt1 = 0 and subsc_cnt3 = 1 ) or subsc_cnt3 >= 1) then 1 end) reg_flag_mtnapp, 0 reg_flag_apps_n_services, 0 sbsc_flag_apps_n_services 
from 
(
select msisdn_key, channel, max(tbl_dt) date_key , sum(case when prev_tbl_dt < yyyymmddRunDate then 1 else 0 end) subsc_cnt1, sum(case when tbl_dt >= yyyymmddRunDate then 1 else 0 end) subsc_cnt2, sum(case when weekid = try_cast(date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') as int) then 1 else 0 end) subsc_cnt3 from ( select coalesce(x.msisdn_key, y.msisdn_key) msisdn_key,coalesce(x.channel, y.channel) channel, x.tbl_dt tbl_dt,weekid,status,y.tbl_dt prev_tbl_dt,cnt 
from 
sbsc_Reg_flags x 
full join 
cvm_db.CVM20_REG_SBSCR_BASE y 
on x.msisdn_key = y.msisdn_key and x.channel= y.channel
where coalesce(x.msisdn_key, y.msisdn_key) is not null) 
group by msisdn_key, channel, weekid 
) 
) a
full join 
(
select msisdn_key,weekid, max(case when upper(channel) like '%USSD%' or upper(channel) like '%SMS%' then 1 else 0 end) vas_flag, max(case when upper(channel) not in ('') then 1 else 0 end) bundle_flag from (select msisdn_key,weekid,channel, count(*) cnt from sbsc_Reg_flags group by msisdn_key,weekid,channel) group by msisdn_key,weekid 
) b 
on a.msisdn_key = b.msisdn_key )
group by coalesce(a.msisdn_key, b.msisdn_key) ,coalesce(a.weekid, b.weekid);
commit;
