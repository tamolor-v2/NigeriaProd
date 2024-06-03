start transaction;
create table flare_8.fb_validation_dq_summary as 
select 
	'Site Info Report' as report_name,
	report_month,
	1.000*sum(valid_record)/sum(total_records) as dq_percentage
from flare_8.fb_site_info_validation
where report_month = firstDayOfMonth 
group by 1,2
union all
select 
	'Cell Info Report' as report_name,
	report_month,
	1.000*sum(valid_record)/sum(total_records) as dq_percentage
from flare_8.fb_cell_info_validation
where report_month = firstDayOfMonth
group by 1,2
union all
select 
	'Site Engagement Report' as report_name,
	report_month,
	1.000*sum(valid_record)/sum(total_records) as dq_percentage 
from flare_8.fb_site_engagement_validation
where report_month = firstDayOfMonth
group by 1,2
union all
select 
	'Population Report' as report_name,
	report_month,
	dq_percentage 
from flare_8.fb_population_dq_rpt 
union all
select 
	'Gateway Info Report' as report_name,
	report_month,
	dq_percentage 
from flare_8.fb_gateway_dq_rpt 
union all
select 
	'Topology Map Report' as report_name,
	report_month,
	dq_percentage 
from flare_8.fb_topology_map_dq_rpt  ;
commit;
