#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_provision_details 
select distinct
           concat('234',subscriber_id) subscriber_id,
           offer_id,
           case when offer_id in (5700,20060) then 'Data'
           else 'Recharge'
           end as offer_desc,
           start_dt,
           expiry_dt,
           tbl_dt
        from  flare_8.offer_dump
        where cast(replace(cast(date_add('day',1,cast(start_dt as date)) as varchar),'-','') as bigint) = tbl_dt
        and tbl_dt = $yest
and offer_id in (5700,5701,20059,20060) " --output-format CSV 

echo "Completed for $yest"

##yest=cast(date_format(date_add('day',+1,date_parse(cast($yest as varchar), '%Y%m%d')),'%Y%m%d') as INT)

