start transaction;
create table flare_8.fb_feed_sla_dq_monthly_temp10 as
with check_date as
(
 SELECT
     try_cast(date_format(CAST(check_date AS DATE),'%Y%m%d') as integer) tbl_dt,
     check_date as date_key,
     feed_name,
     feed_sla,
     opco,
	 feed_proper_name, 
	 feed_frequency,
	 source,
     case when feed_sla > 0 then feed_sla-1+try_cast(date_format(CAST(check_date AS DATE) +interval '1' month ,'%Y%m%d') as integer)
     	  else feed_sla end as feed_sla_date
 FROM
     (select 
     		*,
         	SEQUENCE(date('firstDayOfWithDashpreviousMonth'),
                   current_date,
                   INTERVAL '1' Month) AS date_array
       from flare_8.dim_fb_feed_files         
     ) 
 CROSS JOIN
     UNNEST(date_array) AS t2(check_date)
     ),
cell_info_2g as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_cell_info_2g_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='CELL_INFO_2G' 
group by 1,2,3,4,5,6,7,8),
cell_info_3g as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_cell_info_3g_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='CELL_INFO_3G' 
group by 1,2,3,4,5,6,7,8),
cell_info_4g as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_cell_info_4g_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='CELL_INFO_4G' 
group by 1,2,3,4,5,6,7,8
),
mtnn_asset_extract_2g_cells as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_asset_extract_2g_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MTNN_ASSET_EXTRACT_2G_CELLS' 
group by 1,2,3,4,5,6,7,8
),
mtnn_asset_extract_3g_cells as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_asset_extract_3g_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MTNN_ASSET_EXTRACT_3G_CELLS' 
group by 1,2,3,4,5,6,7,8 
),
mtnn_asset_extract_4g_cells as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_asset_extract_4g_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MTNN_ASSET_EXTRACT_4G_CELLS' 
group by 1,2,3,4,5,6,7,8   
),
MAPS_SITE_INFO as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_MAPS_SITE_INFO_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MAPS_SITE_INFO' 
group by 1,2,3,4,5,6,7,8  
),
topology_map as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_topology_map_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='TOPOLOGY_MAP' 
group by 1,2,3,4,5,6,7,8  
),
CELL_QOS_2G as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(max(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	sum(case when file_received='Y' then 1 else 0 end ) as received_files, 
	sum(case when file_received='N' then 1 else 0 end ) as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_CELL_QOS_2G_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='CELL_QOS_2G' 
group by 1,2,3,4,5,6,7,8 
),
CELL_QOS_3G as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(max(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	sum(case when file_received='Y' then 1 else 0 end ) as received_files, 
	sum(case when file_received='N' then 1 else 0 end ) as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_CELL_QOS_3G_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='CELL_QOS_3G' 
group by 1,2,3,4,5,6,7,8     
),
CELL_QOS_4G as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(max(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	sum(case when file_received='Y' then 1 else 0 end ) as received_files, 
	sum(case when file_received='N' then 1 else 0 end ) as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_CELL_QOS_4G_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='CELL_QOS_4G' 
group by 1,2,3,4,5,6,7,8       
),
MTN_NG_4G_MONTHLY as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_MTN_NG_4G_MONTHLY_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MTN_NG_4G_MONTHLY' 
group by 1,2,3,4,5,6,7,8         
),
MTN_NG_3G_MONTHLY as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_MTN_NG_3G_MONTHLY_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MTN_NG_3G_MONTHLY' 
group by 1,2,3,4,5,6,7,8   
),
POPULATION_DS as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_population_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='POPULATION_DS' 
group by 1,2,3,4,5,6,7,8  
),
GATEWAY_INFORMATION as 
( 
select 	
	dm.tbl_dt as report_month,
	dm.date_key,
	dm.source,
	dm.feed_name,
	case 
		when feed_sla_date <>0 then date_format(date_parse(try_cast(dm.feed_sla_date as varchar),'%Y%m%d'),'%Y-%m-%d') 
		else null end as feed_sla_date,
	dm.opco,
	dm.feed_proper_name, 
	dm.feed_frequency,
	date_format(from_unixtime(try_Cast(min(file_date) as bigint)/1000),'%Y-%m-%d')  as file_received_date,
	1.000*avg(dq_percentage) as dq_index,
	0 as received_files, 
	0 as missing_files, 
	case when dm.feed_frequency='Daily' then count(*) else 1 end as expected_files
from 
	check_date dm
	left join
	flare_8.fb_gateway_dq_rpt a1 
		on a1.table_name=dm.feed_name
		and dm.tbl_dt=a1.report_month
	where upper(dm.feed_name)='MTN_NG_GATEWAY_INFORMATION' 
group by 1,2,3,4,5,6,7,8  
)
select 	
	report_month,
	date_key,
	source,
	feed_name,
	feed_sla_date,
	opco,
	feed_proper_name,
	feed_frequency,
	file_received_date,
	dq_index,
	case 
		when feed_frequency ='Daily' then received_files
		when feed_frequency ='Monthly' and file_received_date is not null then 1 
		else 0
		end as received_files ,
	case 
		when feed_frequency ='Daily' then missing_files
		when feed_frequency ='Monthly' and file_received_date is not null then 0 
		when feed_frequency ='Monthly' and file_received_date is null and current_date > date_parse(feed_sla_date,'%Y-%m-%d') then 1
		else 0
		end as missing_files ,
	case 
		when feed_frequency ='Daily' and missing_files>0 then '#DC5B57'  
		when feed_frequency ='Monthly' and file_received_date is null and current_date > date_parse(feed_sla_date,'%Y-%m-%d') then '#DC5B57'  
		else null
		end as missing_files_RAG ,
	expected_files,
	case 
	    when feed_frequency ='Daily' and missing_files>0 then 'N'
	    when feed_frequency ='Daily' and missing_files=0 then 'Y'
		when feed_frequency ='Monthly' and file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')<=date_parse(feed_sla_date,'%Y-%m-%d') then 'Y' 
		when feed_frequency ='Monthly' and file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')>date_parse(feed_sla_date,'%Y-%m-%d') then 'N' 
		when feed_frequency ='Monthly' and file_received_date is null and current_date > date_parse(feed_sla_date,'%Y-%m-%d') then 'N'
		else null 
		end as in_sla ,
	case
	    when feed_frequency ='Daily' and missing_files>0 then '#DC5B57'  
	    when feed_frequency ='Daily' and missing_files=0 then '#5CBE9A' 
		when feed_frequency ='Monthly' and file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')<=date_parse(feed_sla_date,'%Y-%m-%d') then '#5CBE9A'  
		when feed_frequency ='Monthly' and file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')>date_parse(feed_sla_date,'%Y-%m-%d') then '#DC5B57'   
		when feed_frequency ='Monthly' and file_received_date is null and current_date > date_parse(feed_sla_date,'%Y-%m-%d') then '#DC5B57'  
		else null 
		end as sla_RAG ,
	case when dq_index is null then null  
		 when dq_index>=0.99 then '#5CBE9A' 
		 when dq_index>=0.95 and dq_index<0.99 then '#DD915F' 
		 when dq_index<0.95 then '#DC5B57'  
		 else null end as DQ_RAG
from 
(select * from cell_info_2g
union all 
select * from cell_info_3g
union all 
select * from cell_info_4g
union all 
select * from mtnn_asset_extract_2g_cells
union all 
select * from mtnn_asset_extract_3g_cells
union all 
select * from mtnn_asset_extract_4g_cells
union all 
select * from MAPS_SITE_INFO
union all 
select * from topology_map
union all 
select * from CELL_QOS_2G
union all 
select * from CELL_QOS_3G
union all 
select * from CELL_QOS_4G
union all 
select * from MTN_NG_4G_MONTHLY
union all 
select * from MTN_NG_3G_MONTHLY
union all 
select * from POPULATION_DS
union all 
select * from GATEWAY_INFORMATION
);
commit;
