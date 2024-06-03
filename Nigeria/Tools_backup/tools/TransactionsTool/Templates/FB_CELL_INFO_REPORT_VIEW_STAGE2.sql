start transaction;

create or replace view flare_8.vw_fb_cell_info_base_update as
with base_2g as
( 
select 
 *,
 case when status = 'INACTIVE' then 1 else 0 end as non_active_status,
 case when status is null then 1 else 0 end as not_in_maps 
from 
 ( 
 select 
  ci.site_id,
  ci.gateway_id,
  ci.ran,
  ci.ci,
  ci.lac,
  ci.cgi,
  ci.eci,
  ci.tac,
  ci.tai,
  ci.enodeb_id,
  ci.ecgi,
  coalesce(upper(b.Status),'INACTIVE')  as      status, 
  coalesce(try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int),ci.frequency_mhz) as   frequency_mhz, 
  coalesce(case when a.average_tx_power_dbm is null and try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) in (1800,900) then 43 
  else a.average_tx_power_dbm end ,  ci.tx_power_dbm)  as tx_power_dbm, 
  ci.operation_date, 
  ci.cell_year,
  ci.missing_site_id,
  ci.missing_cgi,
  ci.incorrect_cgi,
  ci.missing_ecgi,
  ci.incorrect_ecgi,
  ci.missing_tac,
  ci.missing_lac,
  ci.missing_frequency,
  ci.missing_tx_power,
  ci.mobile_site ,
  ci.missing_operation_date,
  ci.active_status,
  ci.missing_from_site_info,
  ci.valid_record
 from flare_8.fb_cell_info_report_base ci
 left join
  (select distinct status,
 case when CGI like '1%'then '62'||CGI 
     when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
     FREQ_BAND,
     site
    from flare_8.CELL_INFO_2G 
    where tbl_dt=( select max(tbl_dt) from flare_8.CELL_INFO_2G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth) 
    and ( cgi is not null and cgi not in ('',' ')) 
 ) b
 on ci.cgi =b.cgi and ci.site_id=b.site and ci.ran='2G'
 left join  
  ( select distinct average_tx_power_dbm,
 case when CGI like '1%'then '62'||CGI 
     when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
     site_id
    from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS  
    where tbl_dt=( select max(tbl_dt) 
  from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS 
  where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)) a
 on ci.site_id=a.site_id and ci.ran='2G'
 and a.cgi = ci.cgi
 where ci.ran='2G' and ci.report_month=firstDayOfpreviousMonth  and valid_record=1 
 )
),
base_3g as
( 
select 
 *,
 case when status = 'INACTIVE' then 1 else 0 end as non_active_status,
 case when status is null then 1 else 0 end as not_in_maps 
from 
 ( 
 select 
  ci.site_id,
  ci.gateway_id,
  ci.ran,
  ci.ci,
  ci.lac,
  ci.cgi,
  ci.eci,
  ci.tac,
  ci.tai,
  ci.enodeb_id,
  ci.ecgi,
  coalesce(upper(b.Status),'INACTIVE')  as      status, 
  coalesce(try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int),ci.frequency_mhz) as   frequency_mhz, 
  coalesce(case when a.tx_power_dbm is null and try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) in (900) then 50.8 
 when a.tx_power_dbm is null and try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) in (2100) then 49
 else a.tx_power_dbm end ,  ci.tx_power_dbm)  as tx_power_dbm, 
  ci.operation_date, 
  ci.cell_year,
  ci.missing_site_id,
  ci.missing_cgi,
  ci.incorrect_cgi,
  ci.missing_ecgi,
  ci.incorrect_ecgi,
  ci.missing_tac,
  ci.missing_lac,
  ci.missing_frequency,
  ci.missing_tx_power,
  ci.mobile_site ,
  ci.missing_operation_date,
  ci.active_status,
  ci.missing_from_site_info,
  ci.valid_record
 from flare_8.fb_cell_info_report_base ci
 left join
  (select distinct status,
 case when CGI like '1%'then '62'||CGI 
     when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
     FREQ_BAND,
     site
    from flare_8.CELL_INFO_3G 
    where tbl_dt=(select max(tbl_dt) from flare_8.CELL_INFO_3G where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
    and ( cgi is not null and cgi not in ('',' '))
 ) b
 on ci.cgi =b.cgi and ci.site_id=b.site and ci.ran='3G'
 left join  
  ( select distinct tx_power_dbm,
 case when CGI like '1%'then '62'||CGI 
     when CGI like '631%'then '621'||substr(CGI,4) else CGI end as cgi,
     site_id2
    from flare_8.MTNN_ASSET_EXTRACT_3G_CELLS  
    where tbl_dt=( select max(tbl_dt) 
  from flare_8.MTNN_ASSET_EXTRACT_3G_CELLS 
  where tbl_dt between firstDayOfMonth and lastDayOfNextMonth))a
 on ci.cgi  = a.cgi and ci.site_id=a.site_id2 and ci.ran='3G'
 where ci.ran='3G' and ci.report_month=firstDayOfpreviousMonth  and valid_record=1
 )
),
base_4g as
( 
select 
 *,
 case when status = 'INACTIVE' then 1 else 0 end as non_active_status,
 case when status is null then 1 else 0 end as not_in_maps 
from 
 (  
 select 
  ci.site_id,
  ci.gateway_id,
  ci.ran,
  ci.ci,
  ci.lac,
  ci.cgi,
  ci.eci,
  ci.tac,
  ci.tai,
  ci.enodeb_id,
  ci.ecgi,
  coalesce(upper(b.Status) , 'INACTIVE') as      status,
  coalesce( try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int), ci.frequency_mhz ) as   frequency_mhz,
  coalesce(case when a.tx_power_dbm is null and b.frequency_mhz in (700,2600) then 52 
 when a.tx_power_dbm is null and b.frequency_mhz in (800,1800) then 49
 else a.tx_power_dbm end ,ci.tx_power_dbm)    as tx_power_dbm, 
  ci.operation_date, 
  ci.cell_year,
  ci.missing_site_id,
  ci.missing_cgi,
  ci.incorrect_cgi,
  ci.missing_ecgi,
  ci.incorrect_ecgi,
  ci.missing_tac,
  ci.missing_lac,
  ci.missing_frequency,
  ci.missing_tx_power,
  ci.mobile_site ,
  ci.missing_operation_date,
  ci.active_status,
  ci.missing_from_site_info,
  ci.valid_record
 from flare_8.fb_cell_info_report_base ci
 left join
 (select 
 *,
 (try_cast(split_part(CGI,'-',3) as bigint)*256)+(try_cast(split_part(CGI,'-',4) as int)) eci,
 concat('621-30-', try_cast((try_cast(split_part(CGI,'-',3) as bigint)*256)+(try_cast(split_part(CGI,'-',4) as int)) as varchar)  ) ecgi,
 split_part(CGI,'-',3) enodeb_id,
 try_cast(regexp_replace(FREQ_BAND, '[a-z+A-Z]') as int)   frequency_mhz,
 concat('621-30-',TAC) tai
 from 
 (
   select *,
  row_number() over (partition by cgi order by firstcollecttime) rnum
  from flare_8.CELL_INFO_4G
  where tbl_dt= (SELECT max(tbl_dt)
  FROM
  flare_8.CELL_INFO_4G
  WHERE (tbl_dt BETWEEN firstDayOfMonth AND lastDayOfNextMonth)
  ) 
  ) where rnum=1
    and (concat('621-30-', TRY_CAST(((TRY_CAST(split_part(CGI, '-', 3) AS bigint) * 256) + TRY_CAST(split_part(CGI, '-', 4) AS int)) AS varchar)) <> '' and concat('621-30-', TRY_CAST(((TRY_CAST(split_part(CGI, '-', 3) AS bigint) * 256) + TRY_CAST(split_part(CGI, '-', 4) AS int)) AS varchar)) is not null)
 ) b 
 on b.ecgi=ci.ecgi
 left join 
 (select distinct *,
 (try_cast(split_part(eCGI,'-',3) as bigint)*256)+(try_cast(split_part(eCGI,'-',4) as int)) eci
 from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS
 where tbl_dt=(select max(tbl_dt) 
  from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS 
  where tbl_dt between firstDayOfMonth and lastDayOfNextMonth))  a
 on cast(a.eci as varchar) = ci.eci
 where ci.ran='4G' and ci.report_month=firstDayOfpreviousMonth  and valid_record=1
 )
),
Site_info as 
(
select  
 site as site_id_status,
 status as site_status,
 operation_date as site_operation_date
from(
  select *,
 row_number() over (partition by site order by operation_date) rnum
  from(
 select 
 bts_2g as site,
 upper(status) as status,
 date_format(date_parse(substr(FirstCollectTime,1,10),'%Y-%m-%d'),'%Y-%m-%d') operation_date
 from flare_8.MAPS_SITE_INFO 
 where site <> '' and  tbl_dt = (select max (tbl_dt) from flare_8.MAPS_SITE_INFO where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
 )
 )
 where rnum=1 
)
select *,
 case when site_status ='INACTIVE' then 1 else 0 end as inactive_site 
from
 (
 select * from base_4g
 union all 
 select * from base_3g
 union all
 select * from base_2g
 ) x 
 left join Site_info y
 on x.site_id=y.site_id_status
;
commit;
