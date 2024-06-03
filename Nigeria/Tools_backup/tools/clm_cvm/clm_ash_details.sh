#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_star_121_ash_details 
select 
 msisdn_key,short_code, channel_name ,
 product_name, product_type, product_subtype,
 status, 
 cast(
 case when charging_amount = 'NA' or trim(charging_amount) = '' then cast( 0 as integer)
 else cast(charging_amount as integer)
 end as decimal(18,2)
 ) as charging_amount, 
 activation_time,  tbl_dt
from flare_8.cis_cdr 
where tbl_dt = $yest
and short_code like '%*121%' " --output-format CSV 

echo "Completed for $yest"

##start_date=cast(date_format(date_add('day',+1,date_parse(cast($start_date as varchar), '%Y%m%d')),'%Y%m%d') as INT)

