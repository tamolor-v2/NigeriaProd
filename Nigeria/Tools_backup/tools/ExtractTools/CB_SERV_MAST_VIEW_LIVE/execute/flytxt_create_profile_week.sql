create table nigeria.cm_profile_week_sit_f as
select a.msisdn_key, a.Subscriber_Birthday, a.account_type, 
a.Consumer_Address, a.Imei, a.Imsi, a.X_ID_SALES_REGION, a.Sales_Region_Zone, 
a.Device_Type, a.lte, a.Phone_Brand, a.Phone_Model, a.Preffered_Language, 
a.First_Name, a.Last_Name, a.MoMo_Pin_Status, a.My_MTN_App_Subscription_Status, a.Update_Date 
from flare_8.vp_cm_profile_week a
where tbl_dt=20190108 and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp) 
and length(try_cast(msisdn_key as varchar)) in (11,13) and a.account_type='PREPAID';
