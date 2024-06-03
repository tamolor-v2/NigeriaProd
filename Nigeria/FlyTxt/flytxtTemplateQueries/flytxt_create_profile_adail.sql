start transaction;
delete from nigeria.cm_profile_adail_sit_f where part_key=99;
insert into nigeria.cm_profile_adail_sit_f
select msisdn_key,date_of_activation,subscriber_status,service_class,dual_sim_handset,
Device_Type,bal_last_day_period,advance_credit,dola,dola_Update_Date,RBT_Service_Class,
VLR_Status,dnd_status,current_segment,last_refill_date,last_refill_value,rgs_status,
service_class_id,profile_Update_Date,99 
from flare_8.vp_cm_profile_adail
where tbl_dt=yyyymmdd and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp)
and length(try_cast(msisdn_key as varchar)) in (11,13) and (account_type='PREPAID' or account_type='POSTPAID');
commit;
