start transaction;
create table flare_8.fb_coverage_dq_rpt_detail as 
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
	 filename_reg||'.'||filename_ext as file_name,
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
       where upper(feed_name)='COVERAGE_MAPS'     
     ) 
 CROSS JOIN
     UNNEST(date_array) AS t2(check_date)
     ),
coverage_maps as 
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
	count(dm.file_name) as expected_files,
	sum(case when file_received='Y' then 1 else 0 end ) as received_files
from 
	check_date dm
	left join
	flare_8.fb_coverage_dq_rpt a1  
		on dm.tbl_dt=a1.report_month
		and a1.file_name=dm.file_name
	where upper(dm.feed_name)='COVERAGE_MAPS'
group by 1,2,3,4,5,6,7,8)
select 	
	*,
	case 
		when file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')<=date_parse(feed_sla_date,'%Y-%m-%d') then 'Y' 
		when file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')>date_parse(feed_sla_date,'%Y-%m-%d') then 'N' 
		when file_received_date is null and current_date > date_parse(feed_sla_date,'%Y-%m-%d') then 'N'
		else null 
		end as in_sla,
	case
		when file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')<date_parse(feed_sla_date,'%Y-%m-%d') then '#5CBE9A' 
		when file_received_date is not null and date_parse(file_received_date,'%Y-%m-%d')>date_parse(feed_sla_date,'%Y-%m-%d') then '#DC5B57'  
		when file_received_date is null and current_date > date_parse(feed_sla_date,'%Y-%m-%d') then '#DC5B57'  
		else null 
		end as sla_RAG,
	case
		when file_received_date is not null and expected_files=received_files then '#5CBE9A' 
		when file_received_date is not null and expected_files>received_files then '#DC5B57'  
		else null 
		end as total_received_RAG
from coverage_maps;
commit;
