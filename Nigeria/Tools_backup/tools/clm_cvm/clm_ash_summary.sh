#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_star_121_ash_summary 
select 
 s.success_count,f.failure_count, dt.tbl_dt
from
(select distinct tbl_dt
	from nigeria.clm_star_121_ash_details 
)dt
left join
(select tbl_dt,
 cast(count(*) as integer) failure_count
	from nigeria.clm_star_121_ash_details
	where lower(status) = 'failure'
	group by tbl_dt
)f on dt.tbl_dt = f.tbl_dt
left join
(select tbl_dt,
 cast(count(*) as integer) success_count
	from nigeria.clm_star_121_ash_details
	where lower(status) = 'success'
	group by tbl_dt
)s on dt.tbl_dt = s.tbl_dt
where dt.tbl_dt=$yest " --output-format CSV 

echo "Completed for $yest"

##start_date=cast(date_format(date_add('day',+1,date_parse(cast($start_date as varchar), '%Y%m%d')),'%Y%m%d') as INT)

