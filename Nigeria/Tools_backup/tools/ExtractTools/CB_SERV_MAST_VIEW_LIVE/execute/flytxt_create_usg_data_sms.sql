create table nigeria.cm_usg_data_sms_sit_f as
select msisdn_key,date_key,data_dou,sms_dou,onnet_sms_dou,
offnet_sms_dou,international_sms_dou,idd_dou,idd_roam_dou,international_data_dou from flare_8.vp_cm_usg_data_sms
where tbl_dt=20190108 and aggr='daily' and dola between 0 and 179 and (is_in_today_sdp or is_in_prevdays_sdp) 
and length(try_cast(msisdn_key as varchar)) in (11,13) and account_type='PREPAID';

