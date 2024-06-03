start transaction;
delete from CVM_DB.CVM20_REV_VOI_SMS_DATA_TMP_STG where tbl_dt=yyyymmddRunDate;
insert into CVM_DB.CVM20_REV_VOI_SMS_DATA_TMP_STG 
(msisdn_key , YearID, monthid , WeekID, week_started, week_ended ,total_rev_data_bundle ,total_rev_combo_bundle ,total_rev_bundle ,rev_digital_service2_musictime ,rev_digital_services ,ic_total_rev ,ic_voice_rev ,ic_sms_rev ,ic_voice_rev_mon ,ic_voice_rev_tue ,ic_voice_rev_wed ,ic_voice_rev_thu ,ic_voice_rev_fri ,ic_voice_rev_sat ,ic_voice_rev_sun ,tbl_dt ) 
select 
msisdn_key
,yearid
,monthid 
,weekid
,week_started
,week_ended
,sum(data_bundle_rev) total_rev_data_bundle
,sum(combo_rev) total_rev_combo_bundle
,sum(total_rev_bundle ) total_rev_voice_bundle
,sum(rev_digital_service2_musictime) rev_digital_service2_musictime
,sum(rev_digital_services) rev_digital_services
,sum( ic_total_rev)ic_total_rev
,sum(ic_voice_rev)ic_voice_rev
,sum( ic_sms_rev ) ic_sms_rev
,sum(ic_voice_rev_mon) ic_voice_rev_mon
,sum(ic_voice_rev_tue) ic_voice_rev_tue
,sum(ic_voice_rev_wed) ic_voice_rev_wed
,sum(ic_voice_rev_thu) ic_voice_rev_thu
,sum(ic_voice_rev_fri) ic_voice_rev_fri
,sum(ic_voice_rev_sat) ic_voice_rev_sat
,sum(ic_voice_rev_sun) ic_voice_rev_sun
,tbl_dt 
from (
select 
msisdn_key
,yearid
,monthid 
,weekid
,week_started
,week_ended
,data_bundle_rev 
,combo_rev
,total_rev_bundle
,0 rev_digital_service2_musictime
,0 rev_digital_services
,0 ic_total_rev 
,0 ic_voice_rev
,0 ic_sms_rev
,0 ic_voice_rev_mon
,0 ic_voice_rev_tue
,0 ic_voice_rev_wed
,0 ic_voice_rev_thu
,0 ic_voice_rev_fri
,0 ic_voice_rev_sat
,0 ic_voice_rev_sun
,tbl_dt 
from cvm_db.cvm20_data_bundle_rev_tmp where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
union all 
select 
msisdn_key
,yearid
,monthid 
,weekid
,week_started
,week_ended 
,0 data_bundle_rev 
,0 combo_rev 
,0 total_rev_bundle
,rev_digital_service2_musictime
,rev_digital_services
,0 ic_total_rev 
,0 ic_voice_rev
,0 ic_sms_rev
,0 ic_voice_rev_mon
,0 ic_voice_rev_tue
,0 ic_voice_rev_wed
,0 ic_voice_rev_thu
,0 ic_voice_rev_fri
,0 ic_voice_rev_sat
,0 ic_voice_rev_sun
,tbl_dt 
from cvm_db.cvm20_digital_services_rev_tmp where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
union all 
select 
msisdn_key 
,yearid 
,monthid
,weekid 
,week_started 
,week_ended
,0 data_bundle_rev 
,0 combo_rev 
,0 total_rev_bundle
,0 rev_digital_service2_musictime
,0 rev_digital_services 
,charge_amount as ic_total_rev 
,charge_amount_voice as ic_voice_rev
,charge_amount_sms as ic_sms_rev
,charge_amount_voice_mon as ic_voice_rev_mon
,charge_amount_voice_tue as ic_voice_rev_tue
,charge_amount_voice_wed as ic_voice_rev_wed
,charge_amount_voice_thu as ic_voice_rev_thu
,charge_amount_voice_fri as ic_voice_rev_fri
,charge_amount_voice_sat as ic_voice_rev_sat
,charge_amount_voice_sun as ic_voice_rev_sun
,tbl_dt 
from cvm_db.cvm20_rev_ic_tmp where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
) group by msisdn_key,yearid,monthid,weekid,week_started,week_ended,tbl_dt;
commit;
start transaction;
insert into CVM_DB.CVM20_REV_VOI_SMS_DATA_TMP_STG 
(msisdn_key , yearid,monthid,WeekId,week_started,week_ended ,rev_sms_onnet ,rev_sms_offnet,rev_sms_int ,rev_sms_roam,rev_sms_other ,rev_sms_total ,rev_data_total,rev_voi_onnet ,rev_voi_offnet,rev_voi_int ,rev_voi_roam,rev_voi_fixed ,rev_voi_out ,rev_voi_in,rev_voi_other ,total_rev_voice_payg,total_rev_data_payg ,rev_voi_total ,tot_rev ,mon_rev ,tue_rev ,wed_rev ,thu_rev ,fri_rev ,sat_rev ,sun_rev ,rev_vas ,rev_digital_service1_ayoba,total_rev_voice_bundle,total_rev_sms_bundle,total_rev_vas_bundle ,total_rev_other_bundle , total_rev_sms_payg,total_rev_other_payg , total_rev_payg ,total_rev_other , max_rev_vce_onnet , max_rev_vce_offnet , max_rev_vce_int , max_rev_data , active_arpu , active_rev_vce_onnet , active_rev_vce_offnet, active_rev_vce_int ,active_rev_data, tbl_dt)
select 
msisdn_key , yearid,monthid,WeekId,week_started,week_ended
,rev_sms_onnet 
,rev_sms_offnet
,rev_sms_int 
,rev_sms_roam
,rev_sms_other 
,rev_sms_total 
,rev_data_total
,rev_voi_onnet 
,rev_voi_offnet
,rev_voi_int 
,rev_voi_roam
,rev_voi_fixed 
,rev_voi_out 
,rev_voi_in
,rev_voi_other 
,total_rev_voice_payg
,total_rev_data_payg 
,rev_voi_total 
,tot_rev 
,mon_rev 
,tue_rev 
,wed_rev 
,thu_rev 
,fri_rev 
,sat_rev 
,sun_rev 
,rev_vas 
,rev_digital_service1_ayoba
,total_rev_voice_bundle
,total_rev_sms_bundle
, cast(null as double) total_rev_vas_bundle
, cast(null as double) total_rev_other_bundle
,total_rev_sms_payg
, cast(null as double)total_rev_other_payg
,total_rev_payg 
, cast(null as double) total_rev_other
, max_rev_vce_onnet
, max_rev_vce_offnet
, max_rev_vce_int
, max_rev_data
, tot_rev active_arpu 
, active_rev_vce_onnet
, active_rev_vce_offnet
, active_rev_vce_int 
,active_rev_data 
,cast( week_started as int)tbl_dt
from (
select 
segment5b5_rev.msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m') monthid ,
date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)week_ended
,sum(coalesce(nigeria.segment5b5_rev.sms_onnet_rev ,0)) rev_sms_onnet
,sum(coalesce(nigeria.segment5b5_rev.sms_offnet_rev,0)) rev_sms_offnet
,sum(coalesce(nigeria.segment5b5_rev.sms_int_rev,0)) rev_sms_int
,sum(coalesce(nigeria.segment5b5_rev.sms_roam_rev ,0)) rev_sms_roam
,cast(null as double) rev_sms_other
,sum(coalesce(nigeria.segment5b5_rev.sms_rev ,0)) rev_sms_total
,sum(coalesce(nigeria.segment5b5_rev.data_rev,0)) rev_data_total
,sum(coalesce(nigeria.segment5b5_rev.voice_onnet_rev ,0)) rev_voi_onnet
,sum(coalesce(nigeria.segment5b5_rev.voice_offnet_rev,0)) rev_voi_offnet
,sum(coalesce(nigeria.segment5b5_rev.voice_int_rev,0)) rev_voi_int
,sum(coalesce(nigeria.segment5b5_rev.voice_roam_rev ,0)) rev_voi_roam
, cast(null as double) rev_voi_fixed
,sum(segment5b5_rev.voice_onnet_rev+ segment5b5_rev.voice_offnet_rev+segment5b5_rev.voice_roam_rev+segment5b5_rev.voice_int_rev)rev_voi_out
,sum(segment5b5_rev.voice_roam_incoming_rev) rev_voi_in
,cast(null as double) rev_voi_other
,sum( coalesce(nigeria.segment5b5_rev.voice_offnet_ma_rev ,0) + coalesce( nigeria.segment5b5_rev.voice_onnet_ma_rev,0) 
+ coalesce( nigeria.segment5b5_rev.voice_int_ma_rev,0) + coalesce( nigeria.segment5b5_rev.voice_roam_out_ma_rev,0)
+ coalesce( nigeria.segment5b5_rev.voice_roam_in_ma_rev,0) ) total_rev_voice_payg
, sum(coalesce(nigeria.segment5b5_rev.voice_rev,0))rev_voi_total 
, sum(coalesce(nigeria.segment5b5_rev.tot_rev , 0)) tot_rev
, sum( case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('MON') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end )mon_rev
,sum(
case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('TUE') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end ) tue_rev
,sum(
case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('WED') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end ) wed_rev
,sum(
case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('THU') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end )thu_rev
,sum(
case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('FRI') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end )fri_rev
,sum(
case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('SAT') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end )sat_rev
,sum(
case when upper(format_datetime(date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in('SUN') then 
coalesce(nigeria.segment5b5_rev.tot_rev , 0) else 0 end ) sun_rev
,sum(coalesce(nigeria.segment5b5_rev.vas_rev ,0))rev_vas
,sum(0) rev_digital_service1_ayoba 
,sum( coalesce( nigeria.segment5b5_rev.sms_offnet_da_chargeable_rev ,0) + coalesce(nigeria.segment5b5_rev.sms_onnet_da_chargeable_rev ,0) 
+ coalesce( nigeria.segment5b5_rev.voice_int_da_chargeable_rev,0) + coalesce( nigeria.segment5b5_rev.voice_roam_out_da_chargeable_rev,0)
+ coalesce( nigeria.segment5b5_rev.voice_roam_in_da_chargeable_rev,0) ) total_rev_voice_bundle
,sum( coalesce(nigeria.segment5b5_rev.sms_offnet_da_chargeable_rev ,0) + coalesce( nigeria.segment5b5_rev.sms_onnet_da_chargeable_rev,0) 
+ coalesce( nigeria.segment5b5_rev.sms_int_da_chargeable_rev,0) + coalesce( nigeria.segment5b5_rev.sms_roam_out_da_chargeable_rev,0)
+ coalesce( nigeria.segment5b5_rev.sms_roam_in_da_chargeable_rev,0) ) total_rev_sms_bundle
,sum( coalesce(nigeria.segment5b5_rev.data_roam_ma_rev ,0) + coalesce(nigeria.segment5b5_rev.data_local_ma_rev ,0) ) total_rev_data_payg
,sum( coalesce(nigeria.segment5b5_rev.sms_offnet_ma_rev ,0) + coalesce( nigeria.segment5b5_rev.sms_onnet_ma_rev,0) 
+ coalesce( nigeria.segment5b5_rev.sms_int_ma_rev,0) + coalesce( nigeria.segment5b5_rev.sms_roam_out_ma_rev,0)
+ coalesce( nigeria.segment5b5_rev.sms_roam_in_ma_rev,0) ) total_rev_sms_payg
, sum( coalesce(nigeria.segment5b5_rev.data_roam_ma_rev ,0) + coalesce(nigeria.segment5b5_rev.data_local_ma_rev ,0) 
+coalesce(nigeria.segment5b5_rev.sms_offnet_ma_rev ,0) + coalesce( nigeria.segment5b5_rev.sms_onnet_ma_rev,0) 
+ coalesce( nigeria.segment5b5_rev.sms_int_ma_rev,0) + coalesce( nigeria.segment5b5_rev.sms_roam_out_ma_rev,0)
+ coalesce( nigeria.segment5b5_rev.sms_roam_in_ma_rev,0)
+ coalesce(nigeria.segment5b5_rev.voice_offnet_ma_rev ,0) + coalesce( nigeria.segment5b5_rev.voice_onnet_ma_rev,0) 
+ coalesce( nigeria.segment5b5_rev.voice_int_ma_rev,0) + coalesce( nigeria.segment5b5_rev.voice_roam_out_ma_rev,0)
+ coalesce( nigeria.segment5b5_rev.voice_roam_in_ma_rev,0)
) total_rev_payg
,max(voice_onnet_rev) max_rev_vce_onnet
,max(voice_offnet_rev)max_rev_vce_offnet
,max(voice_int_rev) max_rev_vce_int
,max(data_rev)max_rev_data
,sum( coalesce(nigeria.segment5b5_rev.voice_onnet_rev,0)) active_rev_vce_onnet
,sum( coalesce( nigeria.segment5b5_rev.voice_offnet_rev,0) )active_rev_vce_offnet
,sum( coalesce(nigeria.segment5b5_rev.voice_int_rev,0) ) active_rev_vce_int 
,sum( coalesce(nigeria.segment5b5_rev.data_rev,0) ) active_rev_data
from nigeria.segment5b5_rev where aggr='daily2'
and segment5b5_rev.tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
group by segment5b5_rev.msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y'),
date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m') , 
date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%v'), 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint), 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint),
cast(date_format(DATE_TRUNC('week', date_parse(CAST(segment5b5_rev.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) 
);
commit;
