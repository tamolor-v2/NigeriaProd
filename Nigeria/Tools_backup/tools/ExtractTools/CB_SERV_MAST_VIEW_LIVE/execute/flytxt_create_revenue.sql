create table nigeria.cm_revenue_sit_f as
select msisdn_key,date_key,refill_count,aspu,total_sms_revenue,total_data_revenue1,total_revenue,total_refill_amount,total_decrement,
total_data_revenue2,total_vas_revenue,DATA_2G_Paygo_Revenue,DATA_2G_Bundled_Revenue,DATA_3G_Paygo_Revenue,
DATA_3G_Bundled_Revenue,DATA_4G_Paygo_Revenue,DATA_4G_Bundled_Revenue,Total_Data_Paygo_Revenue,
Total_Data_Bundled_Revenue,International_SMS_Revenue,International_Data_Revenue,Total_IC_Revenue 
from flare_8.vp_cm_revenue
where tbl_dt=20190108 and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp)  
and length(try_cast(msisdn_key as varchar)) in (11,13) and account_type='PREPAID';
