start transaction;

delete from cvm_db.CVM20_SUBS_TMP ;
insert into cvm_db.CVM20_SUBS_TMP
(msisdn_key,YearID,monthid,WeekID,week_started,week_ended,tariff_type,subscriber_type,number_of_sim,sim_type,lga_of_origin_desc,state_of_origin_desc,mnp_ind,activation_dt,churn_date,val_seg,dola,total_rgs90,tbl_dt)
select 
msisdn_key, YearID,monthid,WeekID,week_started,week_ended,tariff_type,subscriber_type,number_of_sim,sim_type,lga_of_origin_desc,state_of_origin_desc,
mnp_ind,activation_dt,churn_date,val_seg,dola,total_rgs90,tbl_dt
from  
(
select sub.msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(sub.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID,                                                                 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m') monthid,
date_format(DATE_TRUNC('week', date_parse(CAST(sub.tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID,                                          
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(sub.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,                 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(sub.tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
,sub.tariff_type
,sub.subscriber_type
,sub.number_of_sim 
,sub.usim sim_type 
,sub.lga lga_of_origin_desc
,sub.state state_of_origin_desc
,case when sub.portstatus not in ('NA') then split_part(sub.portstatus,'-',1) else sub.portstatus end   mnp_ind
,sub.activation_dt
,sub.last_gross_churn_dt churn_date
,sub.value_band val_seg
,sub.dola
,total_rgs90
, cast(  date_format(DATE_TRUNC('week', date_parse(CAST(sub.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) tbl_dt 
, row_number() over (partition by sub.msisdn_key order by sub.tbl_dt desc ) rnk
from 
nigeria.segment5b5_sub sub   
where  sub.aggr='daily' 
and sub.tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
)
where rnk=1;
commit;
