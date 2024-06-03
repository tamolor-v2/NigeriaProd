start transaction;
create table flare_8.fb_CELL_QOS_2G_dq_rpt9 as
with check_date as
(SELECT
     try_cast(date_format(CAST(check_date AS DATE),'%Y%m%d') as integer) tbl_dt,
     check_date as date_key
 FROM
     (VALUES
         (SEQUENCE(date('firstDayOfWithDashpreviousMonth'),
                   current_date,
                   INTERVAL '1' DAY)
         )
     ) AS t1(date_array)
 CROSS JOIN
     UNNEST(date_array) AS t2(check_date)),
cell_info_test as ( 
select 

	case when SITE in ( null,'') then 1 else 0 end as missing_site,
	case when cgi in ( null,'') then 1 
		 when cgi not like '621%' or length(cgi)< 14  then 1  
	else 0 end as incorrect_cgi,	
	case when cell_2g in ( null,'') then 1 else 0 end as missing_cell,
	*
from flare_8.cell_qos_2g 
where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer) 
),
cell_info_dups as ( 

select tbl_dt,count(*) as duplicates
from ( 
	select tbl_dt,time,cell_2g,cgi,count(*) as duplicates
	from flare_8.CELL_QOS_2G 
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
		count(*) as rejected_records 
from flare_8.rejecteddata 
where file_date between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
    and msgtype = 'com.mtn.messages.cell_qos_2g'
group by 1),
file_received as ( 
select 
		tbl_dt, 
		min(try_Cast(moddate as bigint)) as file_date,
		count(*) as file_received
from (select try_cast(date_format( 
							date_add('day',-1,date_parse( 
								 						substr(split(name,'_')[4],1,8)  
							 							,'%Y%m%d')) 
		 			,'%Y%m%d') as integer)
		as tbl_dt,
		moddate,
		name
		from audit.files_filesopps_summary 
		where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
		and upper(name) like '%CELL_QOS_2G%')
group by 1),
total_records as ( 
select tbl_dt,count(*) as total_records 
		from flare_8.CELL_QOS_2G 
		where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer) 
		group by 1
		)
select 
	aa.tbl_dt,
	try_cast(date_format( 
					date_trunc('month', date_parse(cast(aa.tbl_dt  as varchar),'%Y%m%d')  )
 			,'%Y%m%d') as integer)  as tbl_month,
	aa.Date_key,
	try_cast(date_format( 
					date_trunc('month', date_parse(cast(aa.tbl_dt  as varchar),'%Y%m%d')  )
 			,'%Y%m%d') as integer)  as report_month,
	ff.file_date,
	'Nigeria' as OpCo,
	'Cell QOS 2G' as feed_name, 
	'CELL_QOS_2G' as table_name, 
	'Daily' as feed_frequency,
	case when file_received>0 then 'Y' 
		else 'N' end  
		as file_received, 
	case when file_received>0 then '#5CBE9A' 
		else '#DC5B57' end  
		as file_received_RAG, 
	total_records,
	rr.rejected_records, 
	case when coalesce(file_received,0)=0 then '#DC5B57'  
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
	case when file_received>0 or total_records>0 then coalesce(missing_cell ,0) else null end as missing_cell, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(missing_cell ,0)=0 then '#5CBE9A' 
		 when 1.000*missing_cell/total_records <= 0.05 then '#DD915F'
		 when 1.000*missing_cell/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as missing_cell_RAG,
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
	case 
		when coalesce(file_received,0)=0 then '#DC5B57'  
		when coalesce(total_records,0) =0 then '#DC5B57' 
		 when total_records/average_7_days >= 1.05 then '#DC5B57'  
		 when total_records/average_7_days <= 0.95 then '#DC5B57'  
		when 1.000*dq_issues/total_records > 0.05 then '#DC5B57'  
	 	when 1.000*duplicates/total_records >= 0.05 then '#DC5B57'  
		when 1.000*missing_cell/total_records >= 0.05 then '#DC5B57'  
		when 1.000*incorrect_cgi/total_records >= 0.05 then '#DC5B57'  
		when 1.000*missing_site/total_records >= 0.05 then '#DC5B57'  
		when coalesce(total_records,0)=0 and rejected_records>0 then '#DC5B57'  
		when 1.000*dq_issues/total_records <= 0.05 then '#DD915F' 
		when 1.000*missing_cell/total_records <= 0.05 then '#DD915F' 
		when 1.000*incorrect_cgi/total_records <= 0.05 then '#DD915F' 
		when 1.000*missing_site/total_records <= 0.05 then '#DD915F' 
		else '#5CBE9A' 	
		end as CELL_QOS_2G,
	'MAPS' as source
from 
( 
select tbl_dt,date_key
from check_date
) aa
left join
( 
select tbl_dt,total_records, array_average(rows) as average_7_days
from 
(	SELECT tbl_dt, total_records,
 		array_agg(total_records) OVER (ORDER BY tbl_dt ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS rows
	FROM 
		total_records)
) zz
on aa.tbl_dt=zz.tbl_dt
left join
(select tbl_dt,count(*) as dq_issues , sum(missing_cell) as missing_cell, sum(incorrect_cgi) as incorrect_cgi, sum(missing_site) as missing_site
from cell_info_test 
where missing_cell+incorrect_cgi+missing_site>0
group by tbl_dt ) xx
on aa.tbl_dt=xx.tbl_dt
left join
(select tbl_dt,duplicates
from cell_info_dups ) yy
on aa.tbl_dt=yy.tbl_dt
left join
(select tbl_dt,rejected_records
from rejected_records ) rr
on aa.tbl_dt=rr.tbl_dt
left join
(select file_date,tbl_dt,file_received
from file_received ) ff
on aa.tbl_dt=ff.tbl_dt
;
commit;
