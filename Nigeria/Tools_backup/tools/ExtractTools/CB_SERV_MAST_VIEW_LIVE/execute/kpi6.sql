start transaction;
delete from kpi.kpi_val where kpi_id='KPI-000006' and tbl_dt=20190108;
insert into kpi.kpi_val
select
cast(20190108 as int) as date_key,
cast('KPI-000006' as varchar) as kpi_name,
cast('Mobile - Net Adds' as varchar) as kpi_desc,
cast(0 as bigint) as kpi000001,
cast(0 as int) as kpi000002,
cast(0.0 as double) as kpi000003,
cast(0 as int) as kpi000004,
cast(0.0 as double) as kpi000005,
cast(count(*) as int) as kpi000006,
cast(0 as bigint) as kpi000007,
cast(0 as bigint) as kpi000008,
cast(0 as bigint) as kpi000009,
cast(0 as bigint) as kpi000010,
cast(0 as bigint) as kpi000011,
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
cast('KPI-000006' as varchar) as kpi_name,
cast(20190108 as int) as date_key
from (
select closing.msisdn_key,closing.dola
from flare_8.customersubject closing
where closing.tbl_dt=20190108
and closing.dola between 0 and 0
and closing.aggr='daily'
and (closing.is_in_today_sdp or closing.is_in_prevdays_sdp)
and not exists (
select  opening.msisdn_key
from flare_8.customersubject opening
where opening.tbl_dt=20190107
and opening.dola between 0 and 89
and opening.aggr='daily'
and (opening.is_in_today_sdp or opening.is_in_prevdays_sdp) and closing.msisdn_key=opening.msisdn_key)
);
commit;
