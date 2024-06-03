start transaction;
create or replace view flare_8.vw_fb_cell_info_new as 
with cellinfo_2G as (
select 
 b.site site_id,
 '0.0.0.0' gateway_id,
 '2G'  ran,
 b.cell_2g ci,
 split_part(b.cgi,'-',3) as lac,
 case when b.CGI like '1%'then '62'||b.CGI 
 when b.CGI like '631%'then '621'||substr(b.CGI,4) else b.CGI end as cgi,
 null eci,
 null tac ,
 null tai ,
 null enodeb_id ,
 null ecgi , 
 upper(b.Status) status,
 try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) frequency_mhz,
 case when a.average_tx_power_dbm is null and try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) in (1800,900) then 43 
 else a.average_tx_power_dbm end as tx_power_dbm,
 date_format(date_parse(substr(b.FirstCollectTime,1,10),'%Y-%m-%d'),'%Y-%m-%d') operation_date, 
 previous_reported_date
from (select * 
 from flare_8.CELL_INFO_2G 
 where tbl_dt=( select max(tbl_dt) 
 from flare_8.CELL_INFO_2G 
 where tbl_dt between firstDayOfMonth and lastDayOfNextMonth
 ) 
 and try_cast(substr(FirstCollectTime,1,4) as int) >=2022 
 and upper(status)='ACTIVE'
 and ( cgi is not null and cgi not in ('',' '))
 ) b 
 left join 
 ( select * from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS 
 where tbl_dt=( select max(tbl_dt) 
 from flare_8.MTNN_ASSET_EXTRACT_2G_CELLS
 where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)) a
 on a.gsm_cell_identity = b.cell_2g
 left join
 (select concat(site_id,cgi) as site_cgi,min(date_key) as previous_reported_date 
 from flare_8.fb_cell_info_report_base where ran='2G' and report_month=firstDayOfpreviousMonth and valid_record=1 group by 1) bs 
 on concat(b.site,b.cgi)=bs.site_cgi
where bs.previous_reported_date is null
),
cellinfo_3G as (
select 
 b.Site  as site_id,
 '0.0.0.0' gateway_id,
 '3G' ran,
 split_part(b.cgi,'-',4) as ci,
 split_part(b.cgi,'-',3) as lac,
 case when b.CGI like '1%'then '62'||b.CGI 
 when b.CGI like '631%'then '621'||substr(b.CGI,4) else b.CGI end as cgi,
 null eci,
 null tac ,
 null tai ,
 null enodeb_id ,
 null ecgi , 
 upper(b.Status ) status,
 try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) frequency_mhz,
 case when a.tx_power_dbm is null and try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) in (900) then 50.8 
 when a.tx_power_dbm is null and try_cast(regexp_replace(b.FREQ_BAND, '[a-z+A-Z]') as int) in (2100) then 49
 else a.tx_power_dbm end as tx_power_dbm,
 date_format(date_parse(substr(b.FirstCollectTime,1,10),'%Y-%m-%d'),'%Y-%m-%d') as operation_date,
 previous_reported_date
from ( select * 
 from flare_8.CELL_INFO_3G 
 where tbl_dt=(select max(tbl_dt) 
 from flare_8.CELL_INFO_3G
 where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
 and try_cast(substr(FirstCollectTime,1,4) as int) >=2022 
 and upper(status)='ACTIVE'
 and ( cgi is not null and cgi not in ('',' '))
 ) b
 left join 
 (select * 
 from flare_8.MTNN_ASSET_EXTRACT_3G_CELLS 
 where tbl_dt=(select max(tbl_dt) 
 from flare_8.MTNN_ASSET_EXTRACT_3G_CELLS 
 where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)) a
 on a.cell_name = b.wcell_3g and a.site_id2=b.site
 left join
 (select concat(site_id,cgi) as site_cgi,min(date_key) as previous_reported_date 
 from flare_8.fb_cell_info_report_base where ran='3G' and report_month=firstDayOfpreviousMonth and valid_record=1 group by 1) bs
 on concat(b.site,b.cgi)=bs.site_cgi
where bs.previous_reported_date is null
)
,
cellinfo_4G as 
(
select 
 b.site site_id,
 '0.0.0.0' gateway_id,
 '4G' ran,
 null as ci,
 null lac,
 b.cgi,
 b.eci, 
 coalesce(b.TAC,a.tac) as TAC,
 concat('621-30-',coalesce(b.TAC,a.tac)) as tai,
 b.enodeb_id, 
 b.ecgi,
 upper(b.status) as status,
 b.frequency_mhz,
 case when a.tx_power_dbm is null and b.frequency_mhz in (700,2600) then 52 
 when a.tx_power_dbm is null and b.frequency_mhz in (800,1800) then 49
 else a.tx_power_dbm end as tx_power_dbm, 
 date_format(date_parse(substr(b.FirstCollectTime,1,10),'%Y-%m-%d'),'%Y-%m-%d') operation_date,
 bs.previous_reported_date
from (select 
 *,
 (try_cast(split_part(CGI,'-',3) as bigint)*256)+(try_cast(split_part(CGI,'-',4) as int)) eci,
 concat('621-30-', try_cast((try_cast(split_part(CGI,'-',3) as bigint)*256)+(try_cast(split_part(CGI,'-',4) as int)) as varchar) ) ecgi,
 split_part(CGI,'-',3) enodeb_id,
 try_cast(regexp_replace(FREQ_BAND, '[a-z+A-Z]') as int) frequency_mhz 
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
 )
 where rnum=1 AND (TRY_CAST("substr"(FirstCollectTime, 1, 4) AS int) >= 2022)
 AND (upper(status) = 'ACTIVE') 
 AND (concat('621-30-', TRY_CAST(((TRY_CAST(split_part(CGI, '-', 3) AS bigint) * 256) + TRY_CAST(split_part(CGI, '-', 4) AS int)) AS varchar)) <>''  and concat('621-30-', TRY_CAST(((TRY_CAST(split_part(CGI, '-', 3) AS bigint) * 256) + TRY_CAST(split_part(CGI, '-', 4) AS int)) AS varchar)) is not null)
 ) b
 left join 
 (select *,
 (try_cast(split_part(eCGI,'-',3) as bigint)*256)+(try_cast(split_part(eCGI,'-',4) as int)) eci
 from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS
 where tbl_dt=(select max(tbl_dt) 
 from flare_8.MTNN_ASSET_EXTRACT_4G_CELLS
 where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)) a
 on a.eci = b.eci
 left join
 (select ecgi,min(date_key) as previous_reported_date from flare_8.fb_cell_info_report_base where ran='4G' and report_month=firstDayOfpreviousMonth and valid_record=1 group by 1) bs
 on b.ecgi=bs.ecgi
where bs.previous_reported_date is null
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
 where site <> '' and tbl_dt = (select max (tbl_dt) from flare_8.MAPS_SITE_INFO where tbl_dt between firstDayOfMonth and lastDayOfNextMonth)
 )
 )
 where rnum=1 
)
select *,
 case when (missing_cgi+ missing_site_id +incorrect_cgi +missing_ecgi+incorrect_ecgi+mobile_site+missing_operation_date+missing_tac+ missing_lac+ missing_frequency+ missing_tx_power+inactive_site)=0
 and status is not null then 1 else 0 end as valid_record
from
(select 
 distinct *,
 try_cast(substring(operation_date,1,4) as integer) as cell_year,
 case when x.site_id in ( null,'') and status='ACTIVE' then 1 else 0 end as missing_site_id,
 case when ran in ('3G','2G') and cgi in ( null,'') and status='ACTIVE' then 1 else 0 end as missing_cgi,
 case when ran in ('3G','2G') and cgi not like '621%' and status='ACTIVE' then 1 else 0 end as incorrect_cgi,
 case when ran in ('4G') and ecgi in ( null,'') and status='ACTIVE' then 1 else 0 end as missing_ecgi,
 case when ran in ('4G') and ecgi not like '621%' and status='ACTIVE' then 1 else 0 end as incorrect_ecgi,
 case when ran='4G' and tac in ( null,'') and status='ACTIVE' then 1 else 0 end as missing_tac,
 case when ran in ('3G','2G') and lac in ( null,'') and status='ACTIVE' then 1 else 0 end as missing_lac,
 case when frequency_mhz is null and status='ACTIVE' then 1 else 0 end as missing_frequency,
 case when tx_power_dbm is null and status='ACTIVE' then 1 else 0 end as missing_tx_power,
 case when x.site_id like 'T60%' then 1 else 0 end as mobile_site ,
 case when operation_date in ( null) and status='ACTIVE' then 1 else 0 end as missing_operation_date, 
 case when site_status ='INACTIVE' then 1 else 0 end as inactive_site,
 case when status in ('ACTIVE' ) then 1 else 0 end as active_status,
 case when status not in ('ACTIVE' ) then 1 else 0 end as non_active_status,
 0 as not_in_maps
from
 (
 select * from cellinfo_4G
 union all 
 select * from cellinfo_3G
 union all 
 select * from cellinfo_2G
 ) x 
 left join Site_info y
 on x.site_id=y.site_id_status)
;
commit;
