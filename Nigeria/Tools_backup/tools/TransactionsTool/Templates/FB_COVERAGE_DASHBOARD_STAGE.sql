start transaction;
create table flare_8.FB_COVERAGE_dq_rpt4 as
with check_date as
(
 SELECT
     try_cast(date_format(CAST(check_date AS DATE),'%Y%m%d') as integer) tbl_month,
     filename_reg||'.'||filename_ext as file_name
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
file_received as ( 
select  
	tbl_dt, 
    try_cast(date_format( 
					date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
 			,'%Y%m%d') as integer) as tbl_month, 
 	substr(name,1,16)||substr(name,33) as name,
	min(file_date) as file_date,
	count(*) as file_received
from 
	(select 
			
			try_Cast(moddate as bigint) as file_date,
			try_cast(substr(split(name,'_')[4],1,8) as integer) as tbl_dt,
			name,
			try_cast(movecopydate as bigint) as movecopydate,
			row_number() over (partition by name order by movecopydate ) rnum
	from audit.files_filesopps_summary 
	where 
		tbl_dt between firstDayOfMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
		and upper(name) like 'MTNN_%_COVERAGE_%')
where rnum=1 and tbl_dt is not null	
group by 1,2,3 order by 1)
select 
	aa.tbl_month,
	date(try_cast(date_format(date_parse(cast(aa.tbl_month  as varchar),'%Y%m%d'),'%Y-%m-%d') as varchar)) as tbl_month_key,
	aa.tbl_month as report_month,
	aa.file_name,
	ff.tbl_dt,
	date(try_cast(date_format(date_parse(cast(ff.tbl_dt  as varchar),'%Y%m%d'),'%Y-%m-%d') as varchar)) as Date_key,
	ff.file_date,
	'Nigeria' as OpCo,
	'Coverage Maps' as feed_name, 
	'Monthly' as feed_frequency,
	case when file_received>0 then 'Y' 
		else 'N' end  
		as file_received, 
	case when file_received>0 then '#5CBE9A' 
		else '#DC5B57'   
		end as file_received_RAG, 
	case when file_received>0 then '#5CBE9A' 
		else '#DC5B57'   
		end as coverage_maps,
	'Asset' as source
from 
( 
select tbl_month,file_name
from check_date
) aa
left join 
( 
select tbl_dt,tbl_month,name,file_date,file_received
from file_received
) ff
on aa.tbl_month=ff.tbl_month
and aa.file_name=ff.name
;
commit;
