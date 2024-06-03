#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_provision_wbo_eligible_detail 
select Distinct msisdn_key,
offer_id,
cast(start_dt as date),
cast(expiry_dt as date),
tbl_dt
from
(select distinct MSISDN_KEY
from nigeria.segment5b5_sub 
where tbl_dt = $yest and aggr='daily' and dola = 15
and  age_band>2  and exclusion_status ='NA' 
and service_class_id in ('19','25','26','30','31','32','33','39','41','42','43','44','46','48','49','135','185','186')
and tenure is not null)rg left join
(select cast(concat('234',subscriber_id) as bigint) msisdn,
offer_id,
start_dt,
expiry_dt,
tbl_dt
from flare_8.offer_dump
where tbl_dt = $yest
and cast(replace(cast(date_add('day',1,cast(start_dt as date)) as varchar),'-','') as bigint) = tbl_dt
and offer_id in (5700, 5701, 20059, 20060)
) dmp on rg.msisdn_key = dmp.msisdn " --output-format CSV 

echo "Completed for $yest"

##yest=cast(date_format(date_add('day',+1,date_parse(cast($yest as varchar), '%Y%m%d')),'%Y%m%d') as INT)

