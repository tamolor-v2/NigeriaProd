start transaction;
create table flare_8.FB_GATEWAY_dq_rpt3 as
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

	case when country <> 'NG' then 1 else 0 end as incorrect_country,
	case when gateway_id in ( null,'0','0.0.0.0') then 1 else 0 end as incorrect_gateway_id,
	case when gatway_ip in ( null,'0','0.0.0.0') then 1 else 0 end as incorrect_gateway_ip,
	*,
   try_cast(date_format( 
					date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
 			,'%Y%m%d') as integer) as tbl_month
from flarestg.mtn_ng_gateway_information 
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
			gateway_id,
			ip_prefix,
			gatway_ip,
			count(*) as duplicates 
	from flarestg.mtn_ng_gateway_information 
	where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
	group by 1,2,3,4,5
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
    and upper(msgtype) like '%GATEWAY_INFO%'
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
			tbl_dt,
			try_cast(movecopydate as bigint) as movecopydate,
			row_number() over (partition by tbl_dt order by	movecopydate ) rnum
	from audit.files_filesopps_summary 
	where 
		tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
		and upper(name) like '%GATEWAY_INFO%')
where rnum=1	
group by 1 order by 1),
total_records as ( 
(select tbl_dt,
	    try_cast(date_format( 
					date_trunc('month', date_parse(cast(tbl_dt  as varchar),'%Y%m%d') )
 			,'%Y%m%d') as integer) as tbl_month, 		
		count(*) as total_records 
 from flarestg.mtn_ng_gateway_information 
 where tbl_dt between firstDayOfpreviousMonth and try_cast(date_format(CAST(current_date AS DATE),'%Y%m%d') as integer)
 group by 1
		))
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
	'Gateway Info' as feed_name, 
	'mtn_ng_gateway_information' as table_name, 
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
	case when file_received>0 or total_records>0 then coalesce(incorrect_country ,0) else null end as incorrect_country, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(incorrect_country ,0)=0 then '#5CBE9A' 
		 when 1.000*incorrect_country/total_records <= 0.05 then '#DD915F'
		 when 1.000*incorrect_country/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as incorrect_country_RAG,
	case when file_received>0 or total_records>0 then coalesce(incorrect_gateway_id ,0) else null end as incorrect_gateway_id, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
		 when coalesce(incorrect_gateway_id ,0)=0 then '#5CBE9A' 
		 when 1.000*incorrect_gateway_id/total_records <= 0.05 then '#DD915F'
		 when 1.000*incorrect_gateway_id/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as incorrect_gateway_id_RAG,
	case when file_received>0 or total_records>0 then coalesce(incorrect_gateway_ip ,0) else null end as incorrect_gateway_ip, 
	case when coalesce(file_received,0)=0 and coalesce(total_records,0)=0 then '#DC5B57'  
	 	 when coalesce(incorrect_gateway_ip ,0)=0 then '#5CBE9A' 
		 when 1.000*incorrect_gateway_ip/total_records <= 0.05 then '#DD915F'
		 when 1.000*incorrect_gateway_ip/total_records >= 0.05 then '#DC5B57'  
		 else '#BE6CB4' end as incorrect_gateway_ip_RAG ,
	case 
		when coalesce(file_received,0)=0 then '#DC5B57'  
		when coalesce(total_records,0) =0 then '#DC5B57' 
		 when total_records/average_7_days >= 1.05 then '#DC5B57'  
		 when total_records/average_7_days <= 0.95 then '#DC5B57'  
		when 1.000*dq_issues/total_records > 0.05 then '#DC5B57'  
	 	when 1.000*duplicates/total_records >= 0.05 then '#DC5B57'  
		when 1.000*incorrect_country/total_records >= 0.05 then '#DC5B57'  
		when 1.000*incorrect_gateway_id/total_records >= 0.05 then '#DC5B57'  
		when 1.000*incorrect_gateway_ip/total_records >= 0.05 then '#DC5B57'  
		when coalesce(total_records,0)=0 and rejected_records>0 then '#DC5B57'  
		when 1.000*dq_issues/total_records <= 0.05 then '#DD915F' 
		when 1.000*incorrect_country/total_records <= 0.05 then '#DD915F' 
		when 1.000*incorrect_gateway_id/total_records <= 0.05 then '#DD915F' 
		when 1.000*incorrect_gateway_ip/total_records <= 0.05 then '#DD915F' 
		else '#5CBE9A' 	
		end as gateway_info,
	'Asset' as source
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
(select tbl_dt,tbl_month,count(*) as dq_issues , sum(incorrect_country) as incorrect_country, sum(incorrect_gateway_id) as incorrect_gateway_id,  sum(incorrect_gateway_ip) as incorrect_gateway_ip
from cell_info_test 
where incorrect_country+incorrect_gateway_id+incorrect_gateway_ip>0
group by 1,2 ) xx
on ff.tbl_dt=xx.tbl_dt
left join
(select tbl_dt,tbl_month,duplicates
from cell_info_dups ) yy
on ff.tbl_dt=yy.tbl_dt
left join
(select tbl_dt,tbl_month,rejected_records
from rejected_records ) rr
on ff.tbl_dt=rr.tbl_dt
;

commit;
