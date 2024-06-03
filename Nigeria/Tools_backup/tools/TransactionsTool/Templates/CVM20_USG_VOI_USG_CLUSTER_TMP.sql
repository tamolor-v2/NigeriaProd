start transaction;
delete from CVM_DB.CVM20_USG_VOI_USG_CLUSTER_TMP where tbl_dt = yyyymmddRunDate;
insert into CVM_DB.CVM20_USG_VOI_USG_CLUSTER_TMP
(msisdn_key ,YearID,monthid,WeekID,week_started,week_ended,morning_voi_out_sec,afternoon_voi_out_sec,evening_voi_out_sec,night_voi_out_sec,peak_voi_out_sec, non_peak_voi_out_sec,KEEP_MY_NUMBER_FLAG, tbl_dt) 
select 
msisdn_key ,YearID,monthid,WeekID,week_started,week_ended,morning_voi_out_sec,afternoon_voi_out_sec,
evening_voi_out_sec,night_voi_out_sec,peak_voi_out_sec, non_peak_voi_out_sec,KEEP_MY_NUMBER_FLAG, cast(week_started as int) 
from 
(
select  
aa.msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y') YearID,                                                                    
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m') monthid,                                                                 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v') WeekID,                                             
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,                 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended  ,
sum(case when aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') and  aa.hr between 4 and 11 then coalesce(duration,0) else 0  end )  morning_voi_out_sec ,
sum(case when aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') and  aa.hr between 12 and 15 then  coalesce(duration,0) else 0   end ) afternoon_voi_out_sec ,
sum(case when aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') and  aa.hr between 16 and 19 then  coalesce(duration,0) else 0   end ) evening_voi_out_sec ,
sum(case when aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') and   (aa.hr > 20 or  aa.hr <  4) then  coalesce(duration,0) else 0   end ) night_voi_out_sec ,
sum(case when aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') and  (aa.hr between 7 and   18)  then  coalesce(duration,0) else 0   end ) peak_voi_out_sec ,
sum(case when aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') and  (aa.hr  < 7  or  aa.hr > 18) then  coalesce(duration,0) else 0   end )  non_peak_voi_out_sec ,
max(case when  split_part(network_type,'|', 4) in ('Keep My Number') then 1 else 0 end )  KEEP_MY_NUMBER_FLAG 
from  nigeria.daas_daily_usage_by_msisdn aa
where date_key between yyyymmddRunDate and yyyyRunDateWeek
and ( aa.product_type in ('Voice - Local','Outgoing Voice - Roaming') or  split_part(network_type,'|', 4) in ('Keep My Number')) 
group by aa.msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y')  ,                                                                    
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m') ,                                                                 
date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%v')  ,                                             
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)  ,                 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(date_key AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  
);
commit;
