#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_provision_summary 
select 
	cast(dat.data as int) as data_count, cast(rcgh.recharge as int) recharge_count,main.tbl_dt
from 
(select distinct tbl_dt 
	from nigeria.clm_provision_details
) main
left join
(select tbl_dt, count(*) data
	from nigeria.clm_provision_details
	where offer_id in (5700,20060)
	group by tbl_dt 
) dat on main.tbl_dt = dat.tbl_dt
left join 
(select tbl_dt, count(*) recharge
	from nigeria.clm_provision_details
	where offer_id in (5701,20059)
	group by tbl_dt 
) rcgh on main.tbl_dt = rcgh.tbl_dt
where main.tbl_dt = $yest
ORDER BY main.tbl_dt " --output-format CSV 

echo "Completed for $yest"

##yest=cast(date_format(date_add('day',+1,date_parse(cast($yest as varchar), '%Y%m%d')),'%Y%m%d') as INT)

