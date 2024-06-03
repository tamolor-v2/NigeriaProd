start transaction;
create table flare_8.fb_report_sla_dq_detail as 
with check_date as
(
 SELECT
     try_cast(date_format(CAST(check_date AS DATE),'%Y%m%d') as integer) tbl_dt,
     check_date as date_key,
     report_name,
     report_sla,
     opco,
     case when report_sla > 0 then report_sla-1+try_cast(date_format(CAST(check_date AS DATE) +interval '1' month ,'%Y%m%d') as integer)
     	  else report_sla end as report_sla_date
 FROM
     (select 
     		*,
         	SEQUENCE(date('firstDayOfWithDashpreviousMonth'),
                   current_date,
                   INTERVAL '1' Month) AS date_array
       from flare_8.dim_fb_reports        
     ) 
 CROSS JOIN
     UNNEST(date_array) AS t2(check_date)
     )
select 
	dm.opco,
	dm.tbl_dt as report_month,
	dm.report_name,
	dm.date_key,
	dm.report_sla_date,
	sla.expected_files,
	sla.received_files,
	report_delivery_date,
	1.00*sla.received_files/sla.expected_files as input_file_percent,
	sla.dq_index,
	case when sla.report_month < lastDayOfCurrentMonth then 'Y' else null end as sent_to_GC,
	case when sla.report_month < lastDayOfCurrentMonth then '#5CBE9A'  
		 else null end as sent_to_GC_RAG,
	case when report_delivery_date <= report_sla_date  then 'Y' else null end as report_in_sla,
	case when report_delivery_date <= report_sla_date  then '#5CBE9A'
		else null 
		end as report_sla_RAG ,	
	case when file_sla_breach>0 then 'N' 
		 when sla.received_files>0 and file_sla_breach=0 then 'Y' 
		 else null end as feed_in_sla,
	case when file_sla_breach>0 then '#DC5B57'  
		 when sla.received_files>0 and file_sla_breach=0 then '#5CBE9A'  
		 else null
		 end as feed_in_sla_RAG,
	case when 1.00*sla.received_files/sla.expected_files <> 1 and file_sla_breach>0 then '#DC5B57'  
		 when 1.00*sla.received_files/sla.expected_files = 1 then '#5CBE9A' 
		 end as input_file_RAG,
	case when dq_index is null then null  
		 when dq_index>=0.99 then '#5CBE9A' 
		 when dq_index>=0.95 and dq_index<0.99 then '#DD915F' 
		 when dq_index<0.95 then '#DC5B57'  
		 else null end as DQ_RAG	
from
	check_date dm
left join 
	(select 
		'Cell Info Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where cell_info_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Site Info Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where site_info_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Cell QoS Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where cell_qos_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Site Engagement Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where site_engagement_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'System Engagement Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where system_engagement_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Topology Map Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where topology_map_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Population Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where population_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Gateway Info Report' as report_name,
		dq.report_month,
		sum(expected_files) as expected_files,
		sum(received_files) as received_files,
		sum(case when in_sla='N' then 1 else 0 end ) as file_sla_breach,
		avg(dq_index) as dq_index
	from flare_8.dim_fb_feed_files ff ,
		 flare_8.fb_feed_sla_dq_monthly dq
	where gateway_report=1
		  and dq.feed_name=ff.feed_name
	group by 1,2
	union all
	select 
		'Coverage Maps' as report_name,
		dq.report_month,
		expected_files,
		received_files,
		case when in_sla='N' then 1 else 0 end  as file_sla_breach,
		case when received_files>0 then 1 else null end as dq_index
	from flare_8.fb_coverage_dq_rpt_detail dq ) sla
on dm.tbl_dt=sla.report_month
and dm.report_name=sla.report_name
left join flare_8.fb_cell_info_report_delivery_date dld
on dm.tbl_dt= firstDayOfMonth 
and dm.report_name=dld.report_name;
commit;
