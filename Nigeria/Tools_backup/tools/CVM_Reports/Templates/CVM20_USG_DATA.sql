start transaction;
delete from   CVM_DB.CVM20_USG_DATA where tbl_dt = yyyymmddRunDate ; 
insert into cvm_db.CVM20_USG_DATA
( msisdn_key,data_dl_kb,data_up_kb,data_kb_2g,data_kb_3g,data_kb_4g,data_in_bundle_kb,data_in_payg_kb,data_free_kb,
data_roam_kb,data_kb,data_session_cnt,data_session_cnt_2g,data_session_cnt_3g,data_session_cnt_4g,data_roam_session_cnt,
data_session_sec,data_session_sec_2g,data_session_sec_3g,data_session_sec_4g,data_roam_sec,weekday_kb,weekend_kb,weekday_session_cnt,
weekend_session_cnt,morning_kb,afternoon_kb,evening_kb,night_kb,peak_hours_kb,non_peak_hours_kb,data_kb_expired,data_session_drop_rate,
data_session_success_rate,yearid,monthid,weekid,week_started,week_ended,tbl_dt) 
select 
sub.msisdn_key   
,data_dl_kb
,data_up_kb
,data_kb_2g
,data_kb_3g
,data_kb_4g
,data_in_bundle_kb
,data_in_payg_kb
,data_free_kb
,data_roam_kb
,data_kb
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
,data_kb_expired
,data_session_drop_rate
,data_session_success_rate 
,sub.YearID ,sub.monthid,sub.WeekID ,sub.week_started  
,sub.week_ended
,sub.tbl_dt
from cvm_db.cvm20_subs_tmp sub 
left join cvm_db.cvm20_usg_data_sms_voi_tmp usg_dat_voi_sms on sub.msisdn_key=usg_dat_voi_sms.msisdn_key and sub.tbl_dt=usg_dat_voi_sms.tbl_dt   
left join cvm_db.cvm20_customersubject_sn_tmp cs2 on sub.msisdn_key=cs2.msisdn_key and sub.tbl_dt=cs2.tbl_dt   
where sub.dola <= 91
and sub.tbl_dt = yyyymmddRunDate; 
commit;
