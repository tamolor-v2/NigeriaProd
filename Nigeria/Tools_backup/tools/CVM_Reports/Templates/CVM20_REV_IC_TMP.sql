start transaction;
delete from CVM_DB.CVM20_REV_IC_TMP where tbl_dt=yyyymmddRunDate;
insert into CVM_DB.CVM20_REV_IC_TMP 
(msisdn_key, yearid,monthid,weekid, week_started,week_ended,charge_amount,charge_amount_voice,charge_amount_sms,charge_amount_voice_mon,  charge_amount_voice_tue,charge_amount_voice_wed,  
charge_amount_voice_thu,charge_amount_voice_fri,charge_amount_voice_sat,  
charge_amount_voice_sun,call_duration, event_count,tbl_dt)
select   
msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID                 
,date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')   monthid                  
,date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID            
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started     
,cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint) week_ended  ,
sum(try_cast(amount as double)) charge_amount,
sum(  case  when (event_type = 'WBS - Voice'  )  then amount else 0 end  ) charge_amount_voice    
,sum( case  when (event_type  = 'WBS - SMS')  then amount else 0 end  ) charge_amount_sms  
,sum( case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('MON') and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end )  charge_amount_voice_mon
,sum(  case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('TUE') and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end ) charge_amount_voice_tue
,sum( case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('WED')  and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end ) charge_amount_voice_wed
,sum( case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('THU') and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end )  charge_amount_voice_thu
,sum(
case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('FRI') and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end )  charge_amount_voice_fri
,sum(
case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('SAT') and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end )  charge_amount_voice_sat
,sum(
case when upper(format_datetime(  date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'),'E')) in  ('SUN') and (event_type = 'WBS - Voice'  ) then 
coalesce(amount , 0)   else 0 end ) charge_amount_voice_sun
,sum(duration) call_duration,
sum(rec_count) event_count  
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt 
from   nigeria.daas_daily_usage_by_msisdn ic
where date_key  between yyyymmddRunDate and yyyyRunDateWeek
and length(cast (msisdn_key as varchar)) = 13
and product_type in ('WBS - Incoming','WBS - Terminating-Incoming')
group by msisdn_key,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int)
,date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y')                   
,date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')                   
,date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v')           
,cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)       
,cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint);
commit;
