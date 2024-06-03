start transaction;
delete from CVM_DB.CVM20_CUSTOMERSUBJECT_TMP where tbl_dt =yyyymmddRunDate;
insert into  CVM_DB.CVM20_CUSTOMERSUBJECT_TMP
(msisdn_key, YearID, monthid, WeekID, week_started, week_ended, opco_business_type, customer_type, cons_type, country, opco_name, ucid, status, b2b_type, churn_date, dnd_status,dola,muc,muc_lat,muc_lon, tbl_dt) 
select 
msisdn_key, YearID, monthid, WeekID, week_started, week_ended, opco_business_type, customer_type, 
cons_type, country, opco_name, ucid, status, b2b_type, churn_date, dnd_status,dola,muc,muc_lat,muc_lon,
tbl_dt
from (
select 
msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID                                                                 
,date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')  monthid                                                                 
, date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID                                          
, cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started                 
, cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended  
, customersubject.opco_business_type
, customersubject.customer_type
, customersubject.cons_type
, customersubject.country
, customersubject.opco_name
, customersubject.uid  ucid
, customersubject.status
, customersubject.b2b_type
, customersubject.churn_date    
, customersubject.dnd_status 
,dola
,flare_8.customersubject.bts_mu_site_id muc
,flare_8.customersubject.bts_mu_lat muc_lat
, flare_8.customersubject.bts_mu_lat muc_lon
, yyyymmddRunDate as tbl_dt 
, row_number() over (partition by msisdn_key order by tbl_dt desc ) rnk
from flare_8.customersubject 
where aggr='daily'
and length(cast(tbl_dt as varchar)) = 8
and  tbl_dt in (yyyymmddRunDate))
where rnk = 1;
commit;
