start transaction;
delete from nigeria.cm_usg_voice_ic_sit_f where part_key=99;
insert into nigeria.cm_usg_voice_ic_sit_f
select msisdn_key,date_key,Onnet_IC_Revenue,Offnet_IC_Revenue,IDD_Roaming_IC_Revenue,IDD_Roaming_Incoming_Usage,
IDD_roaming_IC_call_count,Onnet_IC_MOU,Offnet_IC_MOU,IDD_IC_MOU,Total_IC_MOU,99 
from flare_8.vp_cm_usg_voice_ic
where tbl_dt=yyyymmdd and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp) 
and length(try_cast(msisdn_key as varchar)) in (11,13) and (account_type='PREPAID' or account_type='POSTPAID');
commit;
