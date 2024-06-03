start transaction;
insert into  cvm_db.cvm20_customersubject_sn_tmp
(msisdn_key, YearID, monthid, WeekID, week_started, week_ended 
,voi_cc_out_count
,voi_cc_out_sec  
,sn_voi_total_mtc  
,sn_voi_total_moc 
,sms_in_count
,data_dl_kb 
,data_up_kb 
,data_session_cnt 
,data_session_cnt_2g 
,data_session_cnt_3g 
,data_session_cnt_4g 
,data_roam_session_cnt  
,data_session_sec 
,data_session_sec_2g 
,data_session_sec_3g 
,data_session_sec_4g  
,data_roam_sec
,weekday_kb
,weekend_kb
,weekday_session_cnt  
,weekend_session_cnt  
,morning_kb  
,afternoon_kb
,evening_kb  
,night_kb 
,peak_hours_kb  
,non_peak_hours_kb 
,airtime_balance_transfer_in_cnt  
,airtime_balance_transfer_out_cnt 
,airtime_balance_transfer_in_amt  
,airtime_balance_transfer_out_amt  
,tbl_dt) 
select 
msisdn_key, YearID, monthid, WeekID, week_started, week_ended  
,voi_cc_out_count
,voi_cc_out_sec  
,sn_voi_total_mtc  
,sn_voi_total_moc  
,sms_in_count
,data_dl_kb 
,data_up_kb 
,data_session_cnt 
,data_session_cnt_2g 
,data_session_cnt_3g 
,data_session_cnt_4g 
,data_roam_session_cnt  
,data_session_sec 
,data_session_sec_2g 
,data_session_sec_3g 
,data_session_sec_4g 
,data_roam_sec 
, weekday_kb
,weekend_kb
,weekday_session_cnt 
,weekend_session_cnt  
,morning_kb  
,afternoon_kb
,evening_kb  
,night_kb 
,peak_hours_kb  
,non_peak_hours_kb 
,airtime_balance_transfer_in_cnt  
,airtime_balance_transfer_out_cnt 
,airtime_balance_transfer_in_amt  
,airtime_balance_transfer_out_amt 
, tbl_dt
from (
select 
msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID  
, date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')monthid
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started  
,cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended  
,sum(coalesce(flare_8.customersubject.voi_cc_out_counter ,0)) voi_cc_out_count 
,sum(coalesce(flare_8.customersubject.voi_cc_out_secs ,0)) voi_cc_out_sec 
,sum(coalesce(flare_8.customersubject.sn_total_mtc ,0)) sn_voi_total_mtc 
,sum(coalesce(flare_8.customersubject.sn_total_moc ,0)) sn_voi_total_moc 
,sum(coalesce(flare_8.customersubject.sms_in_count ,0)) sms_in_count
,sum(coalesce(flare_8.customersubject.data_dl_kb,0)) data_dl_kb
,sum(coalesce(flare_8.customersubject.data_up_kb,0)) data_up_kb
,sum(coalesce(flare_8.customersubject.data_session_cnt,0)) data_session_cnt
,sum(coalesce(flare_8.customersubject.data_session_cnt_2g,0)) data_session_cnt_2g
,sum(coalesce(flare_8.customersubject.data_session_cnt_3g,0)) data_session_cnt_3g
,sum(coalesce(flare_8.customersubject.data_session_cnt_4g,0)) data_session_cnt_4g
,sum(coalesce(flare_8.customersubject.data_roam_session_cnt,0))  data_roam_session_cnt
,sum(coalesce(flare_8.customersubject.data_session_drtn_secs,0)) data_session_sec
,sum(coalesce(flare_8.customersubject.data_session_drtn_secs_2g,0)) data_session_sec_2g
,sum(coalesce(flare_8.customersubject.data_session_drtn_secs_3g,0)) data_session_sec_3g
,sum(coalesce(flare_8.customersubject.data_session_drtn_secs_4g,0)) data_session_sec_4g
,sum(coalesce(flare_8.customersubject.data_roam_drtn_secs,0)) data_roam_sec
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then 
coalesce( flare_8.customersubject.data_kb,0)  else 0 end ) weekday_kb
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('SAT','SUN') then 
coalesce( flare_8.customersubject.data_kb,0)  else 0 end )  weekend_kb
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then
coalesce( flare_8.customersubject.data_session_cnt,0)  else 0 end )  weekday_session_cnt,
sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('SAT','SUN') then 
coalesce( flare_8.customersubject.data_session_cnt,0)  else 0 end ) weekend_session_cnt,
sum(coalesce( flare_8.customersubject.hr4_kb ,0)+ coalesce( flare_8.customersubject.hr5_kb ,0)
+ coalesce( flare_8.customersubject.hr6_kb ,0)+ coalesce( flare_8.customersubject.hr7_kb ,0)
+ coalesce( flare_8.customersubject.hr8_kb ,0)+ coalesce( flare_8.customersubject.hr9_kb ,0)
+ coalesce( flare_8.customersubject.hr10_kb ,0)+ coalesce( flare_8.customersubject.hr11_kb ,0) ) 
morning_kb,
sum(coalesce( flare_8.customersubject.hr12_kb ,0)+ coalesce( flare_8.customersubject.hr13_kb ,0)+ coalesce( flare_8.customersubject.hr14_kb ,0)+ coalesce( flare_8.customersubject.hr15_kb ,0) ) afternoon_kb,
sum(coalesce( flare_8.customersubject.hr16_kb ,0)+ coalesce( flare_8.customersubject.hr17_kb ,0)+ coalesce( flare_8.customersubject.hr18_kb ,0)+ coalesce( flare_8.customersubject.hr19_kb ,0) )  evening_kb,
sum(coalesce( flare_8.customersubject.hr20_kb ,0)+ coalesce( flare_8.customersubject.hr21_kb ,0)+ coalesce( flare_8.customersubject.hr22_kb ,0)+ coalesce( flare_8.customersubject.hr23_kb ,0) + coalesce( flare_8.customersubject.hr0_kb ,0)+ coalesce( flare_8.customersubject.hr1_kb ,0)+ coalesce( flare_8.customersubject.hr2_kb ,0)+ coalesce( flare_8.customersubject.hr3_kb ,0)) night_kb,
sum(coalesce( flare_8.customersubject.hr7_kb ,0)+ coalesce( flare_8.customersubject.hr8_kb ,0)+ coalesce( flare_8.customersubject.hr9_kb ,0)+ coalesce( flare_8.customersubject.hr10_kb ,0) + coalesce( flare_8.customersubject.hr11_kb ,0)+ coalesce( flare_8.customersubject.hr12_kb ,0)+ coalesce( flare_8.customersubject.hr13_kb ,0)+ coalesce( flare_8.customersubject.hr14_kb ,0)+ coalesce( flare_8.customersubject.hr15_kb ,0)+ coalesce( flare_8.customersubject.hr16_kb ,0)+ coalesce( flare_8.customersubject.hr17_kb ,0)+ coalesce( flare_8.customersubject.hr18_kb ,0)+ coalesce( flare_8.customersubject.hr19_kb ,0)) peak_hours_kb,
sum(coalesce( flare_8.customersubject.hr0_kb ,0)+ coalesce( flare_8.customersubject.hr1_kb ,0)+ coalesce( flare_8.customersubject.hr2_kb ,0)+ coalesce( flare_8.customersubject.hr3_kb ,0) + coalesce( flare_8.customersubject.hr4_kb ,0)+ coalesce( flare_8.customersubject.hr5_kb ,0)+ coalesce( flare_8.customersubject.hr6_kb ,0)+ coalesce( flare_8.customersubject.hr20_kb ,0)+ coalesce( flare_8.customersubject.hr21_kb ,0)+ coalesce( flare_8.customersubject.hr22_kb ,0)+ coalesce( flare_8.customersubject.hr23_kb ,0))  non_peak_hours_kb
,sum(coalesce (flare_8.customersubject.balance_transfer_in_cnt,0  )) airtime_balance_transfer_in_cnt
,sum(coalesce (flare_8.customersubject.balance_transfer_out_cnt,0)) airtime_balance_transfer_out_cnt
,sum(coalesce (flare_8.customersubject.balance_transfer_in_rev,0)) airtime_balance_transfer_in_amt
,sum(coalesce (flare_8.customersubject.balance_transfer_out_rev,0)) airtime_balance_transfer_out_amt
, cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt  
from flare_8.customersubject 
where aggr='daily'
and length(cast(tbl_dt as varchar)) = 8
and tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
group by msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') 
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v')  
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) 
,cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint) 
 ,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int)
) ;
commit;
