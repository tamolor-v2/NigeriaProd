start transaction;
delete from kpi.kpi_val where kpi_id='KPI-000013' and tbl_dt=20190108;
insert into kpi.kpi_val
select
cast(20190108 as int) as date_key,
cast('KPI-000013' as varchar) as kpi_name,
cast('Monthly MOU per Subscriber' as varchar) as kpi_desc,
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
cast(0 as bigint) as kpi000011,
cast(0 as bigint) as kpi000012,
cast(round(mou/closing_base,4) as double) as kpi000013,
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
cast('KPI-000013' as varchar) as kpi_name,
cast(20190108 as int) as date_key
from
(select 
round(cast(sum(
voi_onnet_out_nb_secs+voi_onnet_out_b_secs+voi_offnet_out_b_secs+voi_offnet_out_nb_secs+voi_int_out_b_secs+voi_int_out_nb_secs
)*1.0 as double)/60,0) mou
from flare_8.customersubject where 
tbl_dt between 
cast(date_format(date_add('day',-29,date_parse(cast(20190108 as varchar),'%Y%m%d')),'%Y%m%d') as int) and 20190108 
and aggr='daily' and (is_in_today_sdp or is_in_prevdays_sdp)) monthly_mou,
(select  cast(count(*)*1.0 as double) closing_base
from flare_8.customersubject
where tbl_dt =20190108
and dola between 0 and 89
and aggr='daily'
and (is_in_today_sdp or is_in_prevdays_sdp)) closing;
commit;
