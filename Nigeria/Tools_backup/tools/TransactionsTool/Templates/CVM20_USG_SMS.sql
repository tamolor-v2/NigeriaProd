start transaction;
delete from cvm_db.cvm20_usg_sms where tbl_dt = yyyymmddRunDate;
insert into cvm_db.cvm20_usg_sms
( msisdn_key,sms_offnet_out_c_count  ,sms_offnet_out_nc_count ,sms_onnet_out_c_count,sms_onnet_out_nc_count  ,
sms_int_out_c_count  ,sms_int_out_nc_count ,sms_offnet_out_b_count  ,sms_offnet_out_nb_count ,sms_onnet_out_b_count,
sms_onnet_out_nb_count  ,sms_int_out_b_count  ,sms_int_out_nb_count ,sms_offnet_out_free_count  ,sms_onnet_out_free_count,
sms_int_out_free_count  ,sms_roam_out_count,sms_roam_out_free_count ,sms_in_count,sms_out_count  ,
weekday_sms_out_count,weekend_sms_out_count,sms_out_count_bundle ,sms_out_count_payg,yearid,monthid,weekid,week_started,week_ended,tbl_dt) 
select 
sub.msisdn_key   
,sms_offnet_out_c_count     
,sms_offnet_out_nc_count    
,sms_onnet_out_c_count      
,sms_onnet_out_nc_count     
,sms_int_out_c_count        
,sms_int_out_nc_count       
,sms_offnet_out_b_count     
,sms_offnet_out_nb_count    
,sms_onnet_out_b_count      
,sms_onnet_out_nb_count     
,sms_int_out_b_count        
,sms_int_out_nb_count       
,sms_offnet_out_free_count  
,sms_onnet_out_free_count   
,sms_int_out_free_count     
,sms_roam_out_count         
,sms_roam_out_free_count    
,cs2.sms_in_count               
,sms_out_count              
,weekday_sms_out_count      
,weekend_sms_out_count      
,sms_out_count_bundle       
,sms_out_count_payg         
,sub.YearID ,sub.monthid,sub.WeekID ,sub.week_started  
,sub.week_ended
,sub.tbl_dt
from cvm_db.cvm20_subs_tmp sub 
left join cvm_db.cvm20_usg_data_sms_voi_tmp usg_dat_voi_sms on sub.msisdn_key=usg_dat_voi_sms.msisdn_key and sub.tbl_dt=usg_dat_voi_sms.tbl_dt   
left join cvm_db.cvm20_customersubject_sn_tmp cs2 on sub.msisdn_key=cs2.msisdn_key and sub.tbl_dt=cs2.tbl_dt   
where sub.dola <= 180
and sub.tbl_dt = yyyymmddRunDate;
commit;
