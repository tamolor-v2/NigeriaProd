start transaction;
delete from nigeria.cm_usg_realtime_sit_f where part_key=99;
insert into nigeria.cm_usg_realtime_sit_f
select 
msisdn_key,date_key,Total_OG_MOU,Total_Data_Paygo_Usage,Total_Data_Bundled_usage,Total_Data_Usage,
DATA_4G_Paygo_Usage,DATA_4G_Bundled_Usage,DATA_3G_Paygo_Usage,DATA_3G_Bundled_Usage,
DATA_2G_Paygo_Usage,DATA_2G_Bundled_Usage,Total_SMS_Count,99 
from flare_8.vp_cm_usg_realtime
where tbl_dt=yyyymmdd and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp)  
and length(try_cast(msisdn_key as varchar)) in (11,13) and (account_type='PREPAID' or account_type='POSTPAID');
commit;
