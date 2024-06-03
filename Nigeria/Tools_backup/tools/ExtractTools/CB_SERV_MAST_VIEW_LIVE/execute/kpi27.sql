start transaction;
delete from kpi.kpi_val where kpi_id='KPI-000027' and tbl_dt=20190108;
insert into kpi.kpi_val
select
cast(20190108 as int) as date_key,
cast('KPI-000027' as varchar) as kpi_name,
cast('Number of Enterprise accounts' as varchar) as kpi_desc,
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
cast(count(distinct a.account_code_n) as bigint) as kpi000027,
cast(0 as bigint) as kpi000028,
cast(0 as bigint) as kpi000029,
cast(0.0 as double) as kpi000030,
cast(0.0 as double) as kpi000031,
cast(0.0 as double) as kpi000032,
cast(replace(date_format(now(),'%Y%m%d%T'),':') as bigint),
cast('KPI-000027' as varchar) as kpi_name,
cast(20190108 as int) as date_key
from flare_8.customersubject a, flare_8.customersubject_other01 b
where a.tbl_dt =20190108
and (a.is_in_today_sdp or a.is_in_prevdays_sdp) and a.dola between 0 and 89
and a.aggr='daily' AND a.aggr=b.aggr and a.tbl_dt=b.tbl_dt and a.msisdn_key=b.msisdn_key and
b.service_class_id in ('300','301','311','312','308','315','317','318','302',
'313','191','319','320','321','325','326','331','332','333','327','328','329','330','323',
'324','186','185','184');
commit;
