start transaction;

create or replace view flare_8.vw_fb_site_info_new as
WITH topology_map as 
(
select 
 SITEID as site_id, 
 LONG longitude_deg, 
 LAT latitude_deg, 
 SITE_TYPE backhaul,
 Site_Tx_Capacity capacity_uplink_kbps,
 Site_Tx_Capacity capacity_downlink_kbps,
 fiber_date
from flare_8.TOPOLOGY_MAP where tbl_dt = (select max (tbl_dt) from flare_8.TOPOLOGY_MAP where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
  ) ,
Site_info as 
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
  case when bts_2g like 'T60%' then 1 else 0 end as mobile_site ,
  date_format(date_parse(substr(FirstCollectTime,1,10),'%Y-%m-%d'),'%Y-%m-%d') operation_date
  from flare_8.MAPS_SITE_INFO 
  where bts_2g <> '' and  tbl_dt = (select max (tbl_dt) from flare_8.MAPS_SITE_INFO where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
  )
 )
 where rnum=1 
)  ,
cell_info_ran as 
(
 select site_id, ran_list as active_ran_list 
 from  (
 select site_id, array_join(array_agg(RAN), '|') as ran_list
 from ( 
  select distinct site as site_id, '2G' RAN from flare_8.CELL_INFO_2G where status='Active' and tbl_dt in (select max(tbl_dt) from flare_8.CELL_INFO_2G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
  union all
  select distinct site as site_id, '3G' RAN from flare_8.CELL_INFO_3G where status='Active' and tbl_dt in (select max(tbl_dt) from flare_8.CELL_INFO_3G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
  union all
  select distinct site as site_id, '4G' RAN from flare_8.CELL_INFO_4G where status='Active' and tbl_dt in (select max(tbl_dt) from flare_8.CELL_INFO_4G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
 )
 group by site_id
 )
)
,
cell_info_freq as 
(
 select site_id,
  case 
  when MAX_FREQ <= 800 then 'ACCELERATED_UPGRADE'
  when MAX_FREQ > 800 then 'BASELINE_UPGRADE' 
  end site_type
 from (
  select site_id, MAX(FREQ_BAND) MAX_FREQ
  from (
   select distinct site as site_id, try_cast(regexp_replace(FREQ_BAND, '[a-z+A-Z]') as int) FREQ_BAND from flare_8.CELL_INFO_2G
   where status='Active' and tbl_dt in (select max(tbl_dt) from flare_8.CELL_INFO_2G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
   union all
   select distinct site as site_id, try_cast(regexp_replace(FREQ_BAND, '[a-z+A-Z]') as int) FREQ_BAND from flare_8.CELL_INFO_3G
   where status='Active' and tbl_dt in (select max(tbl_dt) from flare_8.CELL_INFO_3G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
   union all
   select distinct site  as site_id, try_cast(regexp_replace(FREQ_BAND, '[a-z+A-Z]') as int) FREQ_BAND from flare_8.CELL_INFO_4G
   where status='Active' and tbl_dt in (select max(tbl_dt) from flare_8.CELL_INFO_4G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
  ) group by site_id
  )
)
select *,
 case when (missing_backhaul+missing_site_id+missing_location+missing_capacity+missing_fibre_date+missing_ran_list+missing_site_type)=0
 and status is not null then 1 else 0 end as valid_record
from
(select 
 si.site_id, 
 tm.longitude_deg, 
 tm.latitude_deg, 
 tm.backhaul, 
 tm.capacity_uplink_kbps,
 tm.capacity_downlink_kbps,
 tm.fiber_date,
 si.status,
 si.site_year,
 si.operation_date,
 cir.active_ran_list,
 cif.site_type,
 cast(date_format(now(), '%Y-%m-%dT%h:%i:%s') as varchar) as date_time,
 mobile_site,
 case when si.site_id in (null,'') and status='ACTIVE' then 1 else 0 end as missing_site_id,
 case when longitude_deg is null and status='ACTIVE' then 1  
  when latitude_deg is null and status='ACTIVE' then 1 else 0  end as missing_location,
 case when backhaul is null and status='ACTIVE' then 1 else 0  end as missing_backhaul,
 case when capacity_uplink_kbps is null and status='ACTIVE' then 1 
  when capacity_downlink_kbps is null and status='ACTIVE' then 1 else 0  end as missing_capacity,
 case when backhaul='Fibre' and fiber_date is null and status='ACTIVE' then 1 else 0  end as missing_fibre_date,
 case when operation_date is null and status='ACTIVE' then 1 else 0  end as missing_operation_date, 
 case when active_ran_list is null and status='ACTIVE' then 1 else 0  end as missing_ran_list,
 case when site_type is null and status='ACTIVE' then 1 else 0  end as missing_site_type,
 case when status not in ('PLANNED',  'ACTIVE',  'INACTIVE',  'DECOMMISSIONED' ) then 1 else 0  end as incorrect_status,
 case when status is null and status='ACTIVE' then 1 else 0  end as missing_status,
 case when status in ('PLANNED' ) then 1 else 0  end as planned_status,
 case when status in ('ACTIVE' ) then 1 else 0  end as active_status,
 case when status in ('INACTIVE') then 1 else 0  end as inactive_status,
 case when status in ('DECOMMISSIONED' ) then 1 else 0  end as decom_status ,
 case when (si.site_id not in (select site_id from flare_8.fb_site_info_report_base where report_month = firstDayOfpreviousMonth and valid_record=1)
  and try_cast(substring(operation_date,1,4) as int)>=2022) then 1 else 0  end as new_site
 from site_info si 
left join topology_map tm on (tm.site_id = si.site_id)
left join cell_info_ran cir on (si.site_id = cir.site_id)
left join cell_info_freq cif on (si.site_id = cif.site_id)
left join (select site_id,min(date_key) as previous_reported_date 
    from flare_8.fb_site_info_report_base where report_month=firstDayOfpreviousMonth and valid_record=1 group by 1) bs on (si.site_id=bs.site_id )
where bs.previous_reported_date is null)
;
commit;
