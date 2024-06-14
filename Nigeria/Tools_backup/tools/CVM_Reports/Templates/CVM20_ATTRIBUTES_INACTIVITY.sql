start transaction;
delete from cvm_db.CVM20_ATTRIBUTES where tbl_dt = yyyymmddRunDate and subject_area = 'Inactivity';
insert into cvm_db.cvm20_attributes 
select  
msisdn_key
, attribute
, attribute_value attribute_value
, null attr_str
, yyyymmddRunDate week_started  
, yyyyRunDateWeek week_ended
, yyyymmddRunDate tbl_dt
,'Inactivity' subject_area
from (select  
msisdn_key
,'last_date_vce_offnet' attribute
,last_voice_offnet_outgoing_evt_dt attribute_value
from nigeria.segment5b5_sub a
where tbl_dt =  yyyyRunDateWeek
and aggr='daily'
and last_voice_offnet_outgoing_evt_dt is not null
union all
select  
msisdn_key,
attribute,
max(attribute_value) attribute_value
from (select  
  msisdn_key
  , 'ds_ayoba_act' attribute
  , try_cast(tbl_dt as bigint) attribute_value
from  flare_8.ayobaevents
where tbl_dt  between yyyymmddRunDate and yyyyRunDateWeek
and   regexp_like(lower(event_name), 'signup')
) group by msisdn_key,attribute
 union all
 select  
 msisdn_key
 ,'last_date_vce_onnet' attribute
 ,last_voice_onnet_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_voice_onnet_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_date_vce_int' attribute
 ,last_voice_international_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_voice_international_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_date_vce_roam' attribute
 ,last_voice_roaming_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_voice_roaming_outgoing_evt_dt is not null
 
 union all
 select  
 msisdn_key
 ,'last_date_sms_offnet' attribute
 ,last_sms_offnet_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_sms_offnet_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_date_sms_onnet' attribute
 ,last_sms_onnet_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_sms_onnet_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_date_sms_int' attribute
 ,last_sms_international_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_sms_international_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_date_sms_roam' attribute
 ,last_sms_roaming_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_sms_roaming_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_digital_rge_dt' attribute
 ,last_digital_activation_outgoing_evt_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_digital_activation_outgoing_evt_dt is not null
 union all
 select  
 msisdn_key
 ,'last_rge_date' attribute
 ,last_rge_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_rge_dt is not null
  union all
 select  
 msisdn_key
 ,'last_data_date' attribute
 ,last_data_rge_dt attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and last_data_rge_dt is not null
union all
 select  
 msisdn_key
 ,'last_data_date_roam' attribute
 ,dola_data_outgoing attribute_value
  from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and dola_data_outgoing is not null
 union all
 select  
 msisdn_key
 ,'dola' attribute
 ,dola attribute_value
 from nigeria.segment5b5_sub a
 where tbl_dt =  yyyyRunDateWeek
 and aggr='daily'
 and dola is not null
 union all
 select  
 msisdn_key
 ,'ds_last_sms_out' attribute
, date_diff('day',date_parse(CAST(last_sms_rge_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
)  
from nigeria.segment5b5_sub a
where tbl_dt =  yyyyRunDateWeek
and aggr='daily'
and last_sms_rge_dt is not null
union all
select  
msisdn_key
,'ds_last_voi_out' attribute
, date_diff('day',date_parse(CAST(last_voice_rge_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
  )  
from nigeria.segment5b5_sub a
where tbl_dt =  yyyyRunDateWeek
and aggr='daily'
and last_voice_rge_dt is not null
union all
select  
msisdn_key
,'ds_last_momo_rge' attribute
, date_diff('day',date_parse(CAST(last_mobile_money_rge_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
  )
from nigeria.segment5b5_sub a
where tbl_dt =  yyyyRunDateWeek
and aggr='daily'
and last_mobile_money_rge_dt is not null
union all
select  
msisdn_key
,'ds_last_recharge_sbsc' attribute
, date_diff('day',date_parse(CAST(last_recharge_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
  )
from nigeria.segment5b5_sub a
where tbl_dt =  yyyyRunDateWeek
and aggr='daily'
and last_recharge_dt is not null
union all
select  
msisdn_key
,'ds_last_recharge_sbsc_mymtn' attribute
, date_diff('day',date_parse(CAST(apprechg_last_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
  )
from nigeria.segment5b5_rch a
where tbl_dt =  yyyyRunDateWeek
and aggr='daily'
and apprechg_last_dt is not null
union all
select  
msisdn_key
,'ds_last_data_out' Attribute
, date_diff('day',date_parse(CAST(last_data_rge_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
  )
 from     nigeria.segment5b5_sub
 where    tbl_dt  = yyyyRunDateWeek
 and last_data_rge_dt is not null
 and aggr='daily'
 and  last_data_outgoing_evt_dt is not null
union all
   select  
msisdn_key
,'ds_last_data_rge' Attribute
, date_diff('day',date_parse(CAST(last_data_rge_dt AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')
 )
from     nigeria.segment5b5_sub
where    tbl_dt  = yyyyRunDateWeek
and last_data_rge_dt is not null
and aggr='daily'
and  last_data_rge_dt is not null
 union all
select
    msisdn_key,
    attribute,
    date_diff('day',date_parse(CAST(attribute_value AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(yyyyRunDateWeek AS varchar(10)), '%Y%m%d')
  ) attribute_value
from (select  
 msisdn_key
 ,concat('ds_last_',lower(substr(trim(split_part(event_type,'-',2)),1,3)),'_in') Attribute
 ,max(date_key) attribute_value
from nigeria.daas_daily_usage_by_msisdn
where date_key between y90day and yyyyRunDateWeek
and     regexp_like(upper(product_type), 'WBS')
and     regexp_like(upper(product_type), 'INCOMING')
group by 1,2
)
union all
select
 msisdn_key,
 attribute,
 date_diff('day',date_parse(CAST(attribute_value AS varchar(10)), '%Y%m%d') ,
date_parse(CAST(yyyyRunDateWeek AS varchar(10)), '%Y%m%d')
    ) attribute_value
  from (select  
 msisdn_key
 ,'ds_last_digital' Attribute
 ,max(date_key)attribute_value
from  nigeria.daas_daily_usage_by_msisdn
where   date_key between y90day and yyyyRunDateWeek
and     upper(event_type) like ('DIGITAL%SERVICE%')
group by 1,2
union all
select 
 msisdn_key
 ,concat('ds_last_airtime_',lower(event_type)) Attribute
 ,max(date_key) attribute_value
 from nigeria.daas_daily_usage_by_msisdn
 where date_key between y90day and yyyyRunDateWeek
 and   regexp_like(upper(event_type), 'RECEIVE|SEND')
 group by 1,2
 union all
 select 
 msisdn_key
 ,case
 when event_type in ('CRBT SUBSCRIPTION','TUNE DOWNLOAD') then 'ds_last_digital_crbt'
 when lower(network_type) like '%call%feel%'  then 'ds_last_digital_Callerfeel'
 when lower(network_type) like '%phone%backup%'     then 'ds_last_digital_PhonebookBackup'
 end Attribute
,max(date_key) attribute_value
from nigeria.daas_daily_usage_by_msisdn
where   date_key between y90day and yyyyRunDateWeek
and     (event_type in ('CRBT SUBSCRIPTION','TUNE DOWNLOAD') or lower(network_type) like '%call%feel%' or lower(network_type) like '%phone%backup%')
group by 1,2
)
); 
commit;
