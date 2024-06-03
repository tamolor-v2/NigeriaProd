start transaction;
delete from flare_8.fb_cell_qos_report_base where report_month=firstDayOfMonth;
insert into flare_8.fb_cell_qos_report_base
with cellqos_2G as (
select 
 firstDayOfMonth as report_month,
 'NG' as country,
 a.site as site_id,
 '2G' ran,
 try_cast(split_part(a.cgi,'-',4) as int)  as ci,
 a.CGI cgi,
 null eci,
 null ecgi,
 (a.data_volume_ps_dl_mtn_2g/1024/1024) data_vol_down_GB,
 (a.data_volume_ps_ul_mtn_2g/1024/1024) data_vol_up_GB, 
 a.Throughput_User_PS_DL_MTN_Kbps tput_down_kbps, 
 a.Throughput_User_PS_UL_MTN_Kbps tput_up_kbps,
 a.Throughput_User_PS_DL_MTN_Kbps tput_user_down_kbps,
 a.Throughput_User_PS_UL_MTN_Kbps tput_user_up_kbps ,
 a.date_time,
 upper(b.status) as cell_status
from (select cast(date_format(from_unixtime(time/1000),'%Y-%m-%dT%H:%i:%s') as varchar) as date_time,
   site,
   case when CGI like '1%'then '62'||CGI 
    when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
   data_volume_ps_dl_mtn_2g,
   data_volume_ps_ul_mtn_2g,
   Throughput_User_PS_DL_MTN_Kbps,
   Throughput_User_PS_UL_MTN_Kbps 
  from  flare_8.CELL_QOS_2G 
   where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth
 and cgi is not null
 and country is not null) a
Left join ( select    
    case when CGI like '1%'then '62'||CGI 
     when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi
     ,status 
     from flare_8.CELL_INFO_2G 
     where tbl_dt=( select max(tbl_dt) 
 from flare_8.CELL_INFO_2G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth) 
 and cgi is not null
 and country is not null) b
on a.CGI = b.CGI
)
,
cellqos_3G as (
select 
 firstDayOfMonth as report_month,
 'NG' as country,
 a.site as site_id,
 '3G' ran,
 try_cast(split_part(a.cgi,'-',4) as int)  as ci,
 a.CGI cgi,
 null eci,
 null ecgi,
 (a.data_volume_ps_dl_mtn_mb_3g/1024) data_vol_down_GB,
 (a.data_volume_ps_ul_mtn_mb_3g/1024) data_vol_up_GB, 
 0 tput_down_kbps, 
 0 tput_up_kbps,
 0 tput_user_down_kbps,
 0 tput_user_up_kbps ,
 a.date_time,
 upper(b.status) as cell_status
from (select cast(date_format(from_unixtime(time/1000),'%Y-%m-%dT%H:%i:%s') as varchar) as date_time,
   site,
   case when CGI like '1%'then '62'||CGI 
    when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
    data_volume_ps_dl_mtn_mb_3g,
    data_volume_ps_ul_mtn_mb_3g
   from  flare_8.CELL_QOS_3G 
   where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth 
   and cgi is not null
 and country is not null) a
Left join ( select case when CGI like '1%'then '62'||CGI 
     when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi
     ,status 
     from flare_8.CELL_INFO_3G 
     where tbl_dt=( select max(tbl_dt) 
 from flare_8.CELL_INFO_3G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth )
 and cgi is not null
 and country is not null) b
on a.CGI = b.CGI
)
,
cellqos_4G as (
select 
 firstDayOfMonth as report_month,
 'NG' as country,
 a.site site_id, 
 '4G' ran,
 null ci,
 null cgi,
 split_part(a.CGI,'-',3) eci,
 a.cgi as  ecgi,
 (a.data_volume_dl_mtn_mb_4g/1024) data_vol_down_GB,
 (a.data_volume_ul_mtn_mb_4g/1024) data_vol_up_GB, 
 a.Throughput_Cell_DL_Mbps tput_down_kbps, 
 a.Throughput_Cell_UL_Mbps tput_up_kbps,
 a.E_UTRAN_IP_Throughput_Cell_DL_MTN_Kbps tput_user_down_kbps,
 a.E_UTRAN_IP_Throughput_Cell_UL_MTN_Kbps tput_user_up_kbps,
 date_time,
 upper(b.status) as cell_status
from (select cast(date_format(from_unixtime(time/1000),'%Y-%m-%dT%H:%i:%s') as varchar) as date_time,
   site,
   case when CGI like '1%'then '62'||CGI 
    when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
    data_volume_dl_mtn_mb_4g,
    data_volume_ul_mtn_mb_4g,
    Throughput_Cell_DL_Mbps,
    Throughput_Cell_UL_Mbps,
    E_UTRAN_IP_Throughput_Cell_DL_MTN_Kbps,
    E_UTRAN_IP_Throughput_Cell_UL_MTN_Kbps
   from  flare_8.CELL_QOS_4G 
   where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth 
   and cgi is not null
   and country is not null) a
Left join ( select case when CGI like '1%'then '62'||CGI 
 when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi
 ,status 
     from flare_8.CELL_INFO_4G 
     where tbl_dt=( select max(tbl_dt) 
 from flare_8.CELL_INFO_4G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
 and cgi is not null
 and country is not null) b
on a.CGI = b.CGI
)
select 
 country,
 site_id,
 ran,
 ci,
 cgi,
 eci,
 ecgi,
 data_vol_down_GB,
 data_vol_up_GB,
 tput_down_kbps,
 tput_up_kbps,
 tput_user_down_kbps,
 tput_user_up_kbps ,
 date_time,
 cell_status,
 missing_site_id,
 missing_ci,
 missing_cgi,
 incorrect_cgi,
 missing_eci,
 missing_ecgi,
 not_on_cellInfo_2g,
 not_on_cellInfo_3g,
 not_on_cellInfo_4g,
 inactive_cells,
 case when (missing_site_id+missing_ci+missing_cgi+incorrect_cgi+missing_eci+missing_ecgi)=0 then 1 else 0 end as valid_record,
 report_month
from
(select 
  distinct *, 
  case when site_id is null then 1 else 0 end as missing_site_id,
  case when ran in ('2G','3G') and ci is null then 1 else 0 end as missing_ci,
  case when ran in ('2G','3G') and cgi is null then 1 else 0 end as missing_cgi,
  case when ran in ('2G','3G') and cgi not like '621%' then 1 else 0 end as incorrect_cgi,
  case when ran in ('4G') and eci is null then 1 else 0 end as missing_eci,
  case when ran in ('4G') and ecgi is null then 1 else 0 end as missing_ecgi,
  case when ran in ('2G') and cell_status is null then 1 else 0 end as not_on_cellInfo_2g,
  case when ran in ('3G') and cell_status is null then 1 else 0 end as not_on_cellInfo_3g,
  case when ran in ('4G') and cell_status is null then 1 else 0 end as not_on_cellInfo_4g,
  case when cell_status='INACTIVE' then 1 else 0 end as inactive_cells 
from
(
 select * from cellqos_2G
union all 
select * from cellqos_3G
union all 
select * from cellqos_4G
));
commit;
