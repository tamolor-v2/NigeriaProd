start transaction;
delete from kpi.kpi_val where kpi_id='KPI-000011' and tbl_dt=20190108;
insert into kpi.kpi_val
select
cast(20190108 as int) as date_key,
cast('KPI-000011' as varchar) as kpi_name,
cast('Mobile - SMS subscribers (Active RGS30)' as varchar) as kpi_desc,
cast(0 as bigint) as kpi000001,
cast(0 as int) as kpi000002,
cast(0.0 as double) as kpi000003,
cast(0 as int) as kpi000004,
cast(0.0 as double) as kpi000005,
cast(0 as int) as kpi000006,
cast(0 as bigint) as kpi000007,
cast(0 as bigint) as kpi000008,
cast(0 as bigint) as kpi000009,
cast(0 as bigint) as kpi000010,
cast(count(distinct msisdn_key) as bigint) as kpi000011,
cast(0 as bigint) as kpi000012,
cast(0.0 as double) as kpi000013,
cast(0.0 as double) as kpi000014,
cast(0.0 as bigint) as kpi000015,
cast(0.0 as double) as kpi000016,
cast(0.0 as double) as kpi000017,
cast(0.0 as double) as kpi000018,
cast(0.0 as double) as kpi000019,
cast(0 as bigint) as kpi000020,
cast(0.0 as double) as kpi000021,
cast(0.0 as double) as kpi000022,
cast(0.0 as double) as kpi000023,
cast(0.0 as double) as kpi000024,
cast(0.0 as double) as kpi000025,
cast(0.0 as double) as kpi000026,
cast(0 as bigint) as kpi000027,
cast(0 as bigint) as kpi000028,
cast(0 as bigint) as kpi000029,
cast(0.0 as double) as kpi000030,
cast(0.0 as double) as kpi000031,
cast(0.0 as double) as kpi000032,
cast(replace(date_format(now(),'%Y%m%d%T'),':') as bigint),
cast('KPI-000011' as varchar) as kpi_name,
cast(20190108 as int) as date_key
from (
select
ce.tbl_dt,
ce.msisdn_key,
ce.last_dt,
et.event_name,
date_diff('day',date_parse(cast(ce.last_dt as varchar),'%Y%m%d'),date_parse(cast(ce.tbl_dt as varchar),'%Y%m%d')) dola
from flare_8.customereventsconsolidated ce, flare_8.event_type_lookup et, flare_8.customersubject cs 
where et.rgs_ind='Y' and ce.event_type=et.event_type and ce.tbl_dt=20190108 and ce.aggr='daily' and ce.aggr=cs.aggr
and ce.tbl_dt=cs.tbl_dt
and ce.msisdn_key=cs.msisdn_key and upper(et.event_name) like '%SMS%' and (cs.is_in_today_sdp or cs.is_in_prevdays_sdp)
) where dola between 0 and 29;
commit;
