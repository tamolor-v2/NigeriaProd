start transaction;
delete from cvm_db.CVM20_SUBS_TMP where tbl_dt =yyyymmddRunDate;
insert into  cvm_db.cvm20_subs_tmp
(msisdn_key, YearID,monthid,WeekID,week_started,week_ended,tariff_type,subscriber_type,customer_type,number_of_sim,sim_type,lga_of_origin_desc,state_of_origin_desc,
mnp_ind,activation_dt,churn_date,val_seg,dola,cgi,site_type,cell_tech,lat,lon,total_rgs90,tbl_dt)
select 
msisdn_key, YearID,monthid,WeekID,week_started,week_ended,tariff_type,subscriber_type,customer_type,number_of_sim,sim_type,lga_of_origin_desc,state_of_origin_desc,
mnp_ind,activation_dt,churn_date,val_seg,dola,cgi,site_type,cell_tech,lat,lon,total_rgs90, tbl_dt
from (
select  msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID,                                                                 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')   monthid ,                                                                 
date_format(DATE_TRUNC('week', date_parse(CAST( tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID,                                          
cast(  date_format(DATE_TRUNC('week', date_parse(CAST( tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,                 
cast(date_format(DATE_TRUNC('week', date_parse(CAST( tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended 
,tariff_type
,subscriber_type 
,customer_type    
,number_of_sim 
,usim sim_type 
,lga lga_of_origin_desc
,state state_of_origin_desc
,mnp_ind
,activation_dt  
,val_seg
,days_to_churn_date 
, as_of 
,churn_date 
,dola
,cgi   
,site_type
,cell_tech
,lat     
,lon  
,total_rgs90 
,cast(  date_format(DATE_TRUNC('week', date_parse(CAST( tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int)   tbl_dt 
,rnk 
from  cvm_db.cvm20_subs_tmp_1  
where rnk=1 and tbl_dt = yyyymmddRunDate
);
commit;
