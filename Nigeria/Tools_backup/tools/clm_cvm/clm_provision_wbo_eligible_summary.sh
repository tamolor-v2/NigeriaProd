#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_provision_wbo_eligible_summary 
select distinct
da.data_count,
rch.recharge_count,
d.tbl_dt
from nigeria.clm_provision_wbo_eligible_detail d
left join
(
select
tbl_dt,count(*)as data_count
from nigeria.clm_provision_wbo_eligible_detail
where offer_id in (5700,20060)
group by tbl_dt
)da on d.tbl_dt = da.tbl_dt
left join
(
select
tbl_dt,count(*)as recharge_count
from nigeria.clm_provision_wbo_eligible_detail
where offer_id in (5701,20059)
group by tbl_dt
)rch on d.tbl_dt = rch.tbl_dt
Where d.tbl_dt=$yest
order by 3  " --output-format CSV 

echo "Completed for $yest"

##yest=cast(date_format(date_add('day',+1,date_parse(cast($yest as varchar), '%Y%m%d')),'%Y%m%d') as INT)

