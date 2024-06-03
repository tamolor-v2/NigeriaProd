start transaction;
create table flare_8.fb_cell_info_4g_dq_rpt9 as 
with check_date as
(SELECT
     try_cast(date_format(CAST(check_date AS DATE),'%Y%m%d') as integer) tbl_month
 FROM
     (VALUES
         (SEQUENCE(date('firstDayOfWithDashpreviousMonth'),
                   current_date,
                   INTERVAL '1' Month)
         )
     ) AS t1(date_array)
 CROSS JOIN
     UNNEST(date_array) AS t2(check_date)),
cell_info_test as ( 
select 

	case when status ='Active' and cell_4g in ( null,'') then 1 else 0 end as incorrect_cell_name,
	case when status ='Active' and cgi in ( null,'') then 1 
		 when status ='Active' and (cgi not like '621%' or length(cgi)< 14 ) then 1  
	else 0 end as incorrect_cgi,
	case when status ='Active' and site in (null,'') then 1  else 0 end as missing_site,
	case when status ='Active' and freq_band in (null,'') then 1  else 0 end as missing_frequency,	
	case when status ='Active' and tac in (null,'') then 1  else 0 end as missing_tac,
	*,
   try_cast(date_format( 
					date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
 			,'%Y%m%d') as integer) as tbl_month
from flare_8.CELL_INFO_4G 
where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)),
cell_info_dups as ( 

select tbl_dt,
	   try_cast(date_format( 
						date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
	 			,'%Y%m%d') as integer) as tbl_month, 
		count(*) as duplicates
from
	(select tbl_dt,
		   try_cast(date_format( 
							date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
		 			,'%Y%m%d') as integer) as tbl_month, 
			cell_4g,
			cgi,
			count(*) as duplicates 
	from flare_8.CELL_INFO_4G 
	where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
	group by 1,2,3,4
	having count(*) > 1)
group by 1
		),
rejected_records as ( 
select  file_date,
		try_cast(date_format( 
							date_add('day',-1,date_parse(cast(file_date  as varchar),'%Y%m%d')) 
		 			,'%Y%m%d') as integer)
		as tbl_dt,
	    try_cast(date_format( 
						date_trunc('month', date_parse(cast(file_date  as varchar),'%Y%m%d') )
	 			,'%Y%m%d') as integer) as tbl_month, 
		count(*) as rejected_records 
from flare_8.rejecteddata 
where file_date between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
    and msgtype = 'com.mtn.messages.cell_info_4g'
group by 1),
file_received as ( 
select  
	tbl_dt, 
    try_cast(date_format( 
					date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
 			,'%Y%m%d') as integer) as tbl_month, 
	min(file_date) as file_date,
	count(*) as file_received
from 
	(select 
			
			try_Cast(moddate as bigint) as file_date,
			try_cast(date_format( 
								date_add('day',-1,date_parse(substr(split(name,'_')[4],1,8),'%Y%m%d')) 
			 			,'%Y%m%d') as integer)
			as tbl_dt,
			try_cast(movecopydate as bigint) as movecopydate,
			row_number() over (partition by try_cast(substr(split(name,'_')[4],1,8) as integer) order by	movecopydate ) rnum
	from audit.files_filesopps_summary 
	where 
		tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
		and upper(name) like '%CELL_INFO_4G%')
where rnum=1	
group by 1 order by 1),
total_records as ( 
select tbl_dt,
	    try_cast(date_format( 
					date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
 			,'%Y%m%d') as integer) as tbl_month, 		
		count(*) as total_records 
 from flare_8.CELL_INFO_4G 
 where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
 group by 1
		)
select 
	aa.tbl_month,
	date(try_cast(date_format(date_parse(cast(aa.tbl_month  as varchar),'%Y%m%d'),'%Y-%m-%d') as varchar)) as tbl_month_key,
	try_cast(date_format( 
							date_add('month',-1,date_parse(cast(aa.tbl_month  as varchar),'%Y%m%d'))  
		 			,'%Y%m%d') as integer) 
		 			as report_month,
	ff.tbl_dt,
	date(try_cast(date_format(date_parse(cast(ff.tbl_dt  as varchar),'%Y%m%d'),'%Y-%m-%d') as varchar)) as Date_key,
	ff.file_date,
	'Nigeria' as OpCo,
	'Cell Info 4G' as feed_name, 
	'CELL_INFO_4G' as table_name, 
	'Monthly' as feed_frequency,
	case when file_received>0 then 'Y' 
		else 'N' end  
		as file_received, 
	case when file_received>0 then '#5CBE9A' 
		else '#DC5B57' end  
		as file_received_RAG, 
	total_records,
	rr.rejected_records, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(rejected_records,0) =0 then '#5CBE9A' 
		 when rejected_records/total_records <= 0.05 then '#DD915F' 
		 when rejected_records/total_records > 0.05 then '#DC5B57'  
		 when coalesce(total_records,0)=0 and rejected_records>0 then '#DC5B57'  
		 else '#BE6CB4' end as Rejected_RAG, 
	try_cast(average_7_days as integer) as average_7_days, 
	case when coalesce(total_records,0) =0 then '#DC5B57' 
		 else ' ' end as average_7_days_RAG, 
	case when coalesce(total_records,0) =0 then '#DC5B57' 
		 when total_records/average_7_days >= 1.05 then '#DC5B57'  
		 when total_records/average_7_days <= 0.95 then '#DC5B57'  
		 else '#5CBE9A' end as Trend_RAG, 
	case when file_received>0 or total_records>0 then coalesce(dq_issues ,0) else null end as dq_issues, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(dq_issues ,0)=0 then '#5CBE9A' 
		 when 1.000*dq_issues/total_records <= 0.05 then '#DD915F' 
		 when 1.000*dq_issues/total_records > 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as DQ_RAG, 
	case when file_received>0 or total_records>0 then 1-coalesce((1.000*dq_issues /total_records),0) else null end as dq_percentage, 
	case when file_received>0 or total_records>0 then coalesce(duplicates ,0) else null end as duplicates, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(duplicates ,0)=0 then '#5CBE9A' 
		 when 1.000*duplicates/total_records <= 0.05 then '#DD915F'
		 when 1.000*duplicates/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as Duplicate_RAG,
	case when file_received>0 or total_records>0 then coalesce(incorrect_cell_name ,0) else null end as incorrect_cell_name, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(incorrect_cell_name ,0)=0 then '#5CBE9A' 
		 when 1.000*incorrect_cell_name/total_records <= 0.05 then '#DD915F'
		 when 1.000*incorrect_cell_name/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as incorrect_cell_name_RAG,
	case when file_received>0 or total_records>0 then coalesce(incorrect_cgi ,0) else null end as incorrect_cgi, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(incorrect_cgi ,0)=0 then '#5CBE9A' 
		 when 1.000*incorrect_cgi/total_records <= 0.05 then '#DD915F'
		 when 1.000*incorrect_cgi/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as incorrect_cgi_RAG,
	case when file_received>0 or total_records>0 then coalesce(missing_site ,0) else null end as missing_site, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
	 	 when coalesce(missing_site ,0)=0 then '#5CBE9A' 
		 when 1.000*missing_site/total_records <= 0.05 then '#DD915F'
		 when 1.000*missing_site/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as missing_site_RAG ,
	case when file_received>0 or total_records>0 then coalesce(missing_frequency ,0) else null end as missing_frequency, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
	 	 when coalesce(missing_frequency ,0)=0 then '#5CBE9A' 
		 when 1.000*missing_frequency/total_records <= 0.05 then '#DD915F'
		 when 1.000*missing_frequency/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as missing_frequency_RAG ,
	case when file_received>0 or total_records>0 then coalesce(missing_tac ,0) else null end as missing_tac, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
	 	 when coalesce(missing_tac ,0)=0 then '#5CBE9A' 
		 when 1.000*missing_tac/total_records <= 0.05 then '#DD915F'
		 when 1.000*missing_tac/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as missing_tac_RAG ,		 
	case 
		when coalesce(file_received,0)=0 then '#DC5B57'  
		when coalesce(total_records,0) =0 then '#DC5B57' 
		 when total_records/average_7_days >= 1.05 then '#DC5B57'  
		 when total_records/average_7_days <= 0.95 then '#DC5B57'  
		when 1.000*dq_issues/total_records > 0.05 then '#DC5B57'  
	 	when 1.000*duplicates/total_records >= 0.05 then '#DC5B57'  
		when 1.000*incorrect_cell_name/total_records >= 0.05 then '#DC5B57'  
		when 1.000*incorrect_cgi/total_records >= 0.05 then '#DC5B57'  
		when 1.000*missing_frequency/total_records >= 0.05 then '#DC5B57'  
		when 1.000*missing_site/total_records >= 0.05 then '#DC5B57'  
		when 1.000*missing_tac/total_records >= 0.05 then '#DC5B57'  
		when coalesce(total_records,0)=0 and rejected_records>0 then '#DC5B57'  
		when 1.000*dq_issues/total_records <= 0.05 then '#DD915F' 
		when 1.000*incorrect_cell_name/total_records <= 0.05 then '#DD915F' 
		when 1.000*incorrect_cgi/total_records <= 0.05 then '#DD915F' 
		when 1.000*missing_frequency/total_records <= 0.05 then '#DD915F' 
		when 1.000*missing_site/total_records <= 0.05 then '#DD915F' 
		when 1.000*missing_tac/total_records <= 0.05 then '#DD915F' 
		else '#5CBE9A' 	
		end as cell_info_4g,
	'MAPS' as source
from 
( 
select tbl_month
from check_date
) aa
left join 
( 
select tbl_dt,tbl_month,file_date,file_received
from file_received
) ff
on aa.tbl_month=ff.tbl_month
left join
( 
select tbl_dt,tbl_month,total_records, array_average(rows) as average_7_days
from 
(	SELECT tbl_dt,tbl_month, total_records,
 		array_agg(total_records) OVER (ORDER BY tbl_dt ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS rows
	FROM 
		total_records)
) zz
on ff.tbl_dt=zz.tbl_dt
left join
(select tbl_dt,count(*) as dq_issues , sum(incorrect_cell_name) as incorrect_cell_name, sum(incorrect_cgi) as incorrect_cgi, sum(missing_frequency) as missing_frequency , sum(missing_tac) as missing_tac, sum(missing_site) as missing_site
from cell_info_test 
where incorrect_cell_name+incorrect_cgi+missing_site+missing_frequency+missing_tac>0
group by 1 ) xx
on ff.tbl_dt=xx.tbl_dt
left join
(select tbl_dt,tbl_month,duplicates
from cell_info_dups ) yy
on ff.tbl_dt=yy.tbl_dt
left join
(select tbl_dt,tbl_month,rejected_records
from rejected_records ) rr
on ff.tbl_dt=rr.tbl_dt;
commit;