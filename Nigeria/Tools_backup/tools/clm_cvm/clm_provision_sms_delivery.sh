#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_provision_sms_delivery 
select distinct
cast(subscriber_id as bigint) msisdn,
offer_id,
offer_desc,
case when start_dt is null then null else cast(Date_parse(cast(start_dt as varchar),'%Y%m%d') as DATE) end start_dt,
case when expiry_dt is null then null else cast(Date_parse(cast(expiry_dt as varchar),'%Y%m%d') as DATE) end expiry_dt,
smsc_cnt,
case when sum(smsc_cnt) > 0 then cast(1 as boolean)
else cast(0 as boolean)
end as is_msg_received,
tbl_dt
from
(select
d.subscriber_id,
d.offer_id,
d.offer_desc,
d.start_dt,
d.expiry_dt,
cast(case when s.smsc_ack_time is not null  then 1 else 0 end as integer) smsc_cnt,
d.tbl_dt
from (select
d.subscriber_id,
d.offer_id,
d.offer_desc,
cast(replace(cast(d.start_dt as varchar),'-','') as integer) start_dt,
cast(replace(cast(d.expiry_dt as varchar),'-','') as integer) expiry_dt,
d.tbl_dt
from nigeria.clm_provision_details d
where tbl_dt = $yest
) d
left join flare_8.flytxt_latch_dump s
on s.tbl_dt >= d.start_dt And s.tbl_dt <= d.expiry_dt
and d.subscriber_id = s.msisdn
) alls Where tbl_dt = $yest
group by
subscriber_id,
offer_id,
offer_desc,
start_dt,
expiry_dt,
smsc_cnt,
tbl_dt  " --output-format CSV 

echo "Completed for $yest"

##start_date=cast(date_format(date_add('day',+1,date_parse(cast($start_date as varchar), '%Y%m%d')),'%Y%m%d') as INT)

