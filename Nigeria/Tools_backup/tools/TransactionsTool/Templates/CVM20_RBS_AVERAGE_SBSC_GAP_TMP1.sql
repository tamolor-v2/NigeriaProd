start transaction;
delete from  cvm_db.CVM20_RBS_AVERAGE_SBSC_GAP_TMP1   where tbl_dt=yyyymmddRunDate;
insert into  cvm_db.cvm20_rbs_average_sbsc_gap_tmp1  
(msisdn_key,weekid,average_sbsc_gap, tbl_dt) 
select 
msisdn_key,
'yyyymmddRunDate'  week_id , 
avg(date_diff ('day',  date_parse(cast(date_diff_days2   AS varchar(10) ),'%Y%m%d'),date_parse(cast(date_diff_days1 AS varchar(10) ),'%Y%m%d') )) average_sbsc_gap,
yyyymmddRunDate  tbl_dt  
from (
SELECT
msisdn_key
, date_key   
, "date_format"("date_trunc"('week', "date_parse"(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')), '%Y%m%d') weekid
, "lag"(date_key) OVER (PARTITION BY msisdn_key ORDER BY date_key desc) dlag_ayid 
,  case when  date_format(lag("date_parse"(CAST(date_key AS varchar(10)), '%Y%m%d')) OVER (PARTITION BY msisdn_key ORDER BY date_key desc),'%Y%m%d') is null 
then date_format(date_parse(CAST(date_key AS varchar(10)),'%Y%m%d'), '%Y%m%d')  else 
date_format(lag("date_parse"(CAST(date_key AS varchar(10)), '%Y%m%d')) OVER (PARTITION BY msisdn_key ORDER BY date_key desc),'%Y%m%d') end 
date_diff_days1 
,   date_format(date_parse(CAST(date_key AS varchar(10)),'%Y%m%d'), '%Y%m%d')  date_diff_days2  
from 
(select msisdn_key,   date_key, sum(totalcharge_money ) totalcharge_money
from cvm_db.cvm20_vw_refill_and_data_bundle_uat a
WHERE   (channel not IN ('EVD', 'DYA', 'VTU')  )  
and "date_format"("date_trunc"('week', "date_parse"(CAST(date_key AS varchar(10)), '%Y%m%d')), '%Y%m%d') 
between    date_format(date_add('week',- 11,date_parse(cast(yyyymmddRunDate as varchar),'%Y%m%d')),'%Y%m%d') 
and  date_format(DATE_TRUNC('week', date_parse(CAST( yyyymmddRunDate  AS varchar(10)), '%Y%m%d')),'%Y%m%d')  
and totalcharge_money > 0
group by msisdn_key, date_key
) ) where dlag_ayid is not null
group by msisdn_key ;
commit;