start transaction;
delete from cvm_db.cvm20_cc_tmp where tbl_dt = yyyymmddRunDate;
insert into  cvm_db.cvm20_cc_tmp
(msisdn_key, YearID,monthid,WeekID,week_started,week_ended,total_resolved_count,total_issue_registered_count,tbl_dt)
select 
msisdn_key, YearID,monthid,WeekID,week_started,week_ended,total_resolved_count,total_issue_registered_count,tbl_dt
from  
(
select cr.msisdn_key,
 date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID,                                                                 
       date_format(date_parse(cast(cr.tbl_dt as varchar),'%Y%m%d'),'%Y%m') monthid,                                                                 
     date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID,                                          
     cast(  date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,                 
     cast(date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
, sum(case when upper(cr.status)  = 'CLOSED'  then 1 else 0 end ) total_resolved_count
, sum(1 )  total_issue_registered_count
, cast(  date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt  
from 
flare_8.call_reason  cr 
where  cr.tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
group by cr.msisdn_key,
 date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y')  ,                                                                 
       date_format(date_parse(cast(cr.tbl_dt as varchar),'%Y%m%d'),'%Y%m')  ,                                                                 
     date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%v')  ,                                          
     cast(  date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)  ,                 
     cast(date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint) 
     , cast(  date_format(DATE_TRUNC('week', date_parse(CAST(cr.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) 
);
commit;
