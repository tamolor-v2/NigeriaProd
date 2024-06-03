start transaction;

create or replace view flare_8.vw_fb_site_engage_234_daily as
with Site_info as 
(
select  
 site as site_id,
 status as status,
 mobile_site,
 try_cast(substring(operation_date,1,4) as int) as site_year,
 operation_date
from(
  select *,
   row_number() over (partition by site order by operation_date) rnum
  from(
   select 
    bts_2g as site,
    upper(status) as status,
    case when site like 'T60%' then 1 else 0 end as mobile_site ,
    date_format(date_parse(substr(FirstCollectTime,1,10),'%Y-%m-%d'),'%Y-%m-%d') operation_date
    from flare_8.MAPS_SITE_INFO 
   where site <> '' and  tbl_dt = (select max (tbl_dt) from flare_8.MAPS_SITE_INFO where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
   )
 )
 where rnum=1 
)  ,
Site_engage_2g as (
select 
 a.*,
 b.status,
 b.mobile_site
from 
(select 
 tbl_dt,
 site as site_id,
 '2G' as ran,
 0 users_monthly,
 0 users_daily,
 sum(data_volume_ps_dl_mtn_2g/1024/1024) as data_vol_down_GB,
 sum(data_volume_ps_ul_mtn_2g/1024/1024) as data_vol_up_GB
from flare_8.CELL_QOS_2G 
where 
 tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth
 and cgi is not null
 and country is not null
group by tbl_dt,site ) a
left join Site_info b
on a.site_id=b.site_id
),
site_engage_3g as (
select 
 a.tbl_dt,
 a.site_id,
 '3G' ran,
 round(coalesce(c.users_hs,0) + coalesce (c.users_ps_hsdpa_mtn,0)) users_monthly,
 round(coalesce(d.users_hs,0) + coalesce (d.users_ps_hsdpa_mtn,0)) users_daily,
 a.data_vol_down_GB,
 a.data_vol_up_GB,
 b.status,
 b.mobile_site
from (
 select 
  tbl_dt,
  site as site_id, 
  sum(data_volume_ps_dl_mtn_mb_3g/1024) as data_vol_down_GB,
  sum(data_volume_ps_ul_mtn_mb_3g/1024) as data_vol_up_GB 
 from  flare_8.CELL_QOS_3G 
 where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth
    and cgi is not null
    and country is not null
 group by tbl_dt,site) a
left join Site_info b
on a.site_id=b.site_id
left join 
 ( select try_cast(regexp_replace(time,'[-]') as int) as date_time,site, users_hs, users_ps_hsdpa_mtn
  from flare_8.mtn_ng_3g_monthly
  where tbl_dt = (select max(tbl_dt) from flare_8.mtn_ng_3g_monthly where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth and time <> 'currentMonthWithDash') 
   and time <> 'currentMonthWithDash' )  d  
 on a.site_id =d.site and a.tbl_dt=d.date_time 
left join 
 ( select lastDayOfCurrentMonth as date_time,  
      site, users_hs, users_ps_hsdpa_mtn
  from flare_8.mtn_ng_3g_monthly  
  where tbl_dt = (select max(tbl_dt) from flare_8.mtn_ng_3g_monthly where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth and time = 'currentMonthWithDash') and time = 'currentMonthWithDash')  c 
 on a.site_id =c.site and a.tbl_dt=c.date_time 
),
site_engage_4g as (
select 
 tbl_dt, 
 a.site_id, 
 '4G' ran,
 round(coalesce(c.rrc_connected_ues_avg_mtn,0) + coalesce (c.rrc_connected_ues_max_mtn,0)) users_monthly,
 round(coalesce(d.rrc_connected_ues_avg_mtn,0) + coalesce (d.rrc_connected_ues_max_mtn,0)) users_daily,
 data_vol_down_GB,
 data_vol_up_GB,
 b.status,
 b.mobile_site
from 
  (
 select 
  tbl_dt,
  site as site_id, 
  sum(data_volume_dl_mtn_mb_4g/1024) as data_vol_down_GB,
  sum(data_volume_ul_mtn_mb_4g/1024) as data_vol_up_GB 
 from  flare_8.CELL_QOS_4G 
 where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth
    and cgi is not null
    and country is not null
 group by tbl_dt,site) a
left join Site_info b
on a.site_id=b.site_id
left join 
 ( select try_cast(regexp_replace(time,'[-]') as int) as date_time,site, rrc_connected_ues_avg_mtn, rrc_connected_ues_max_mtn
  from flare_8.mtn_ng_4g_daily  
  where tbl_dt in (select max(tbl_dt) from flare_8.mtn_ng_4g_monthly where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth and time <> 'currentMonthWithDash') and time <> 'currentMonthWithDash')  d 
 on a.site_id =d.site 
left join 
 ( select lastDayOfCurrentMonth as date_time,site, rrc_connected_ues_avg_mtn, rrc_connected_ues_max_mtn
  from flare_8.mtn_ng_4g_monthly  
  where tbl_dt = (select max(tbl_dt) from flare_8.mtn_ng_4g_monthly where tbl_dt between firstDayOfMonth and lastDayOfCurrentMonth and time = 'currentMonthWithDash') and time = 'currentMonthWithDash')  c  
 on a.site_id =c.site and a.tbl_dt=c.date_time 
)
select 'NG' as country,
		*,
	   case when (missing_site_id+mobile_site+missing_status)=0 then 1 else 0 end as valid_record
from
(select 
	distinct * , 
	cast(date_format(date_parse(cast(tbl_dt as varchar),'%Y%m%d'), '%Y-%m-%dT%h:%i:%s') as varchar) as date_time,
	case when site_id in (null,'') and status='ACTIVE' then 1 else 0 end as missing_site_id,
	case when status is null then 1 else 0 end as missing_status,
	case when status is null then 1 else 0 end as missing_maps_site_info
from 
	(
	select * from site_engage_2g 
	union all
	select * from  site_engage_3g 
	union all
	select * from site_engage_4g 
	));
commit;
