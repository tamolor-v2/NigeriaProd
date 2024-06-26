start transaction;
insert into CVM_DB.CVM20_REV_VOI_SMS_DATA_TMP
(msisdn_key,yearid ,monthid,weekid ,week_started ,week_ended,rev_sms_onnet,rev_sms_offnet,rev_sms_int,rev_sms_roam ,rev_sms_other,rev_sms_total,rev_data_total,rev_voi_onnet,rev_voi_offnet,rev_voi_int,rev_voi_roam ,rev_voi_fixed,rev_voi_out,rev_voi_in,rev_voi_other,total_rev_voice_payg,total_rev_data_payg,rev_voi_total,tot_rev,mon_rev,tue_rev,wed_rev,thu_rev,fri_rev,sat_rev,sun_rev,rev_vas,rev_digital_service1_ayoba,rev_digital_service2_musictime ,rev_digital_services,total_rev_voice_bundle,total_rev_data_bundle ,total_rev_combo_bundle,total_rev_sms_bundle,total_rev_bundle,total_rev_vas_bundle,total_rev_other_bundle,total_rev_sms_payg ,total_rev_other_payg,total_rev_payg,total_rev_other ,max_rev_vce_onnet,max_rev_vce_offnet ,max_rev_vce_int ,max_rev_data ,active_arpu,active_rev_vce_onnet,active_rev_vce_offnet ,active_rev_vce_int ,active_rev_data ,tbl_dt)
select 
msisdn_key,
yearid ,
monthid,
weekid ,
week_started ,
week_ended,
sum(coalesce(rev_sms_onnet,0)) rev_sms_onnet,
sum(coalesce(rev_sms_offnet  ,0)) rev_sms_offnet  ,
sum(coalesce(rev_sms_int  ,0)) rev_sms_int  ,
sum(coalesce(rev_sms_roam ,0)) rev_sms_roam ,
sum(coalesce(rev_sms_other,0)) rev_sms_other,
sum(coalesce(rev_sms_total,0)  + coalesce(ic_sms_rev,0) ) rev_sms_total,
sum(coalesce(rev_data_total ,0) ) rev_data_total  ,
sum(coalesce(rev_voi_onnet  ,0) ) rev_voi_onnet,
sum(coalesce(rev_voi_offnet ,0) ) rev_voi_offnet  ,
sum(coalesce(rev_voi_int ,0) ) rev_voi_int  ,
sum(coalesce(rev_voi_roam,0) ) rev_voi_roam ,
sum(coalesce(rev_voi_fixed  ,0) ) rev_voi_fixed,
sum(coalesce(rev_voi_out ,0) ) rev_voi_out  ,
sum(coalesce(rev_voi_in,0)  + coalesce(ic_total_rev,0)  ) rev_voi_in,
sum(coalesce(rev_voi_other,0)) rev_voi_other,
sum(coalesce(total_rev_voice_payg,0)  ) total_rev_voice_payg  ,
sum(coalesce(total_rev_data_payg ,0)  ) total_rev_data_payg,
sum(coalesce(rev_voi_total,0)  + coalesce(ic_voice_rev,0)) rev_voi_total,
sum(coalesce(tot_rev ,0) + coalesce(ic_total_rev,0) )tot_rev ,
sum(coalesce(mon_rev ,0)+ coalesce(ic_voice_rev_mon,0) )  mon_rev  ,
sum(coalesce(tue_rev ,0)+ coalesce(ic_voice_rev_tue,0) ) tue_rev,
sum(coalesce(wed_rev ,0)+ coalesce(ic_voice_rev_wed,0) ) wed_rev ,
sum(coalesce(thu_rev ,0)+ coalesce(ic_voice_rev_thu,0) ) thu_rev ,
sum(coalesce(fri_rev ,0)+ coalesce(ic_voice_rev_fri,0) ) fri_rev ,
sum(coalesce(sat_rev ,0)+ coalesce(ic_voice_rev_sat,0) ) sat_rev ,
sum(coalesce(sun_rev ,0)+ coalesce(ic_voice_rev_sun,0) ) sun_rev  ,
sum(coalesce(rev_vas,0)) rev_vas,
sum(coalesce(rev_digital_service1_ayoba  ,0)) rev_digital_service1_ayoba  ,
sum(coalesce(rev_digital_service2_musictime ,0)) rev_digital_service2_musictime ,
sum(coalesce(rev_digital_services  ,0)) rev_digital_services  ,
sum(coalesce(total_rev_voice_bundle,0)) total_rev_voice_bundle,
sum(coalesce(total_rev_data_bundle ,0)) total_rev_data_bundle ,
sum(coalesce(total_rev_combo_bundle,0)) total_rev_combo_bundle,
sum(coalesce(total_rev_sms_bundle  ,0)) total_rev_sms_bundle  ,
sum(coalesce(total_rev_sms_bundle ,0)
+ coalesce(total_rev_voice_bundle,0) 
+ coalesce(total_rev_data_bundle,0) ) total_rev_bundle, 
sum(coalesce(total_rev_vas_bundle  ,0)) total_rev_vas_bundle  ,
sum(coalesce(total_rev_other_bundle,0)) total_rev_other_bundle,
sum(coalesce(total_rev_sms_payg ,0)) total_rev_sms_payg ,
sum(coalesce(total_rev_other_payg  ,0)) total_rev_other_payg  ,
sum(coalesce(total_rev_payg,0)  ) total_rev_payg  ,
sum(coalesce(total_rev_other,0) ) total_rev_other ,
max(coalesce(max_rev_vce_onnet,0)  ) max_rev_vce_onnet  ,
max(coalesce(max_rev_vce_offnet,0) ) max_rev_vce_offnet ,
max(coalesce(max_rev_vce_int,0) ) max_rev_vce_int ,
max(coalesce(max_rev_data,0) ) max_rev_data  ,
sum(coalesce(active_arpu,0) + coalesce(ic_total_rev,0) ) active_arpu ,
sum(coalesce(active_rev_vce_onnet,0)  ) active_rev_vce_onnet  ,
sum(coalesce(active_rev_vce_offnet,0) ) active_rev_vce_offnet ,
sum(coalesce(active_rev_vce_int,0) ) active_rev_vce_int ,
sum(coalesce(active_rev_data ,0)) active_rev_data ,
tbl_dt 
from cvm_db.cvm20_rev_voi_sms_data_tmp_stg  
where tbl_dt = yyyymmddRunDate 
group by  msisdn_key,yearid ,monthid,weekid ,week_started ,week_ended,tbl_dt;
commit;
