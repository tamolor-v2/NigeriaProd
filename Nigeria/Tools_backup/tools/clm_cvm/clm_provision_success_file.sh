#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_provision_success_file
	select distinct cast(msisdn as bigint)msisdn,
         offer_id,
         offer_desc,
         start_dt,
         expiry_dt,
         case when sum(success_count) > 0 then cast(1 as boolean)
         else cast(0 as boolean)
         end is_successful,
         tbl_dt
from
(select
                d.subscriber_id msisdn,
                d.offer_id,
                d.offer_desc,
                d.start_dt,
                d.expiry_dt,
                cast(case
                when d.offer_id in ( 5700,20060) and gprs.msisdn_key is not null  then 1
                when d.offer_id in ( 5701,20059) and air.msisdn_key is not null  then 1
                else 0
                end as integer) success_count,
                d.tbl_dt
                from nigeria.clm_provision_details d
                left join flare_8.cs5_ccn_gprs_ma gprs
                on gprs.tbl_dt>=cast(replace(cast(d.start_dt as varchar),'-','') as integer) and gprs.tbl_dt<=cast(replace(cast(d.expiry_dt as varchar),'-','') as integer) 
                and gprs.tbl_dt  = $yest
                and  cast (d.subscriber_id as varchar) = cast(gprs.msisdn_key as varchar)
                left join flare_8.cs5_air_refill_ma air
                on air.tbl_dt>=cast(replace(cast(d.start_dt as varchar),'-','') as integer) and air.tbl_dt<=cast(replace(cast(d.expiry_dt as varchar),'-','') as integer)
                and air.tbl_dt = $yest
                and  cast (d.subscriber_id as varchar) = cast(air.msisdn_key as varchar)
                ) alls where alls.tbl_dt=$yest
                group by
                        msisdn,
                        offer_id,
                        offer_desc,
                        start_dt,
                        expiry_dt,
                        tbl_dt " --output-format CSV

echo "Completed for $yest"

##yest=cast(date_format(date_add('day',+1,date_parse(cast($yest as varchar), '%Y%m%d')),'%Y%m%d') as INT)

