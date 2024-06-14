start transaction;
delete from  CVM_DB.CVM20_DIGITAL_SERVICES_REV_TMP where tbl_dt=yyyymmddRunDate;
insert into CVM_DB.CVM20_DIGITAL_SERVICES_REV_TMP
select 
msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')   monthid ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID,  
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
,sum( case when upper(nigeria.daas_daily_usage_by_msisdn.network_type) like '%MUSIC%TIME%' then amount else 0 end ) rev_digital_service2_musictime
,sum( case when  upper(event_type) like '%DIGITAL%SERVICE%' then amount else 0 end ) rev_digital_services
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt 
from nigeria.daas_daily_usage_by_msisdn 
where date_key  between yyyymmddRunDate and yyyyRunDateWeek
group by msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y'),
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m')  ,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v')  , 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)  ,                 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint),
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) ;
commit;
