#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into ng_ops_support.cta_offer_dump_latch 
select a.msisdn_key,a.offer_id,start_dt Offer_Start_Date,expiry_dt Offer_End_Date,
case when b.msisdn is not null then 'Latch' else 'SMS PUSH' end Uptake_Mode,
b.hlr_latch_time,b.smsc_ack_time smsc_del_time,b.time_diff,a.tbl_dt
from flare_8.offer_dump a
left join Flare_8.flytxt_latch_dump b on (a.msisdn_key = try_cast(b.msisdn as bigint) and a.tbl_dt = b.tbl_dt)
where a.tbl_dt = $yest
and  a.offer_id = 5702;

insert into ng_ops_support.cta_offer_dump_refill 
select c.msisdn_key,originnodetype Channel,try_cast(transactionamount as double) transactionamount,origin_time_stamp Recharge_Time,c.tbl_dt
from flare_8.cs5_air_refill_ma c
join ng_ops_support.cta_offer_dump_latch a
on (a.msisdn_key = c.msisdn_key and a.tbl_dt = c.tbl_dt 
and c.tbl_dt between try_cast(date_format(date_parse(Offer_Start_Date,'%Y-%m-%d'),'%Y%m%d') as int) 
and try_cast(date_format(date_parse(Offer_End_Date,'%Y-%m-%d'),'%Y%m%d') as int))
And c.tbl_dt = $yest;

insert into nigeria.customer_retention_latch_cube 
select a.msisdn_key,offer_id,offer_start_date,offer_end_date,uptake_mode,hlr_latch_time,
smsc_del_time,time_diff,channel,transactionamount,recharge_time,a.tbl_dt
from ng_ops_support.cta_offer_dump_latch a
left join ng_ops_support.cta_offer_dump_refill c
on (a.msisdn_key = c.msisdn_key and a.tbl_dt = c.tbl_dt)
where a.tbl_dt = $yest; " --output-format CSV 

echo "Completed for $yest"

##yest=cast(date_format(date_add('day',+1,date_parse(cast($yest as varchar), '%Y%m%d')),'%Y%m%d') as INT)

