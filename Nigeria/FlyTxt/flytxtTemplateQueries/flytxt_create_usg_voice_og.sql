start transaction;
delete from nigeria.cm_usg_voice_og_sit_f where part_key=99;
insert into nigeria.cm_usg_voice_og_sit_f
select 
msisdn_key,date_key,Onnet_OG_Free_MOU,Onnet_OG_Paid_MOU,Offnet_OG_Free_MOU,Offnet_OG_Paid_MOU,
Local_OG_MOU,IDD_OG_MOU,Voice_DOU,Onnet_OG_Revenue,Offnet_OG_Revenue,Local_OG_Revenue,
IDD_OG_Revenue,Total_OG_Revenue,IDD_Roaming_OG_Usage,IDD_Roaming_OG_Revenue,IDD_Roaming_OG_Call_count,
Total_DOU,profile_Update_Date,99
from flare_8.vp_cm_usg_voice_og
where tbl_dt=yyyymmdd and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp) 
and length(try_cast(msisdn_key as varchar)) in (11,13) and (account_type='PREPAID' or account_type='POSTPAID');
commit;
