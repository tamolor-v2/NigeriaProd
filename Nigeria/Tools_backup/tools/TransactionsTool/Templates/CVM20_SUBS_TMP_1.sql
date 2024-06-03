start transaction;
delete from cvm_db.CVM20_SUBS_TMP_1 where tbl_dt= yyyymmddRunDate;
insert into cvm_db.CVM20_SUBS_TMP_1
(msisdn_key,tariff_type,subscriber_type,customer_type,number_of_sim,usim,lga,state,mnp_ind,activation_dt,val_seg,days_to_churn_date,
as_of,churn_date,dola,cgi,site_type,cell_tech,lat,lon,total_rgs90,week_part,rnk,tbl_dt)
select 
msisdn_key,tariff_type,subscriber_type,customer_type,number_of_sim,usim,lga,state,mnp_ind,activation_dt,val_seg,days_to_churn_date,
as_of,churn_date,dola,cgi,site_type,cell_tech,lat,lon,total_rgs90,week_part,rnk,tbl_dt
from 
(
select msisdn_key, yyyymmddRunDate as tbl_dt, tariff_type
,coalesce(vcd.subscriber_type,'','UNKNOWN') subscriber_type
,coalesce(vcd.customer_type,'','NA')   customer_type 
,number_of_sim,usim  
,sub.lga  
,sub.state  
,case when sub.portstatus not in ('NA') then split_part(sub.portstatus,'-',1) else sub.portstatus end   mnp_ind
,sub.activation_dt  
,sub.value_band val_seg
,(89-dola) days_to_churn_date 
, yyyyRunDateWeek as_of 
,cast(date_format(date_add('day',(90-dola),date_parse(cast(yyyyRunDateWeek as varchar),'%Y%m%d')),'%Y%m%d') as int) churn_date 
,sub.dola
,sub.cgi   
,dim_cell.site_type
,dim_cell.cell_tech
,dim_cell.lat     
,dim_cell.lon  
,total_rgs90
, cast(  date_format(DATE_TRUNC('week', date_parse(CAST(sub.tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int) week_part 
, 1 as rnk
from 
nigeria.segment5b5_sub sub   
left join flare_8.vp_dim_cellsite dim_cell   on sub.cgi =dim_cell.cdr_cgi  
left join dim.vp_cvm_dim_package_ng  vcd  on sub.service_class_id =vcd.package_cd 
where  sub.aggr='daily'  
and sub.tbl_dt = yyyyRunDateWeek
and barring_type <> 'BARRED'
and  dola <= 180);  
commit;
