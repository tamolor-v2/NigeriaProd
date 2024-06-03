#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_latching_dashboard 
select
ls.tbl_dt,
eli.eligibility_count,
prov.provisioned_count,
sms.sms_delivery_count latch_sms_delivery_count,
suc.success_count,
--latch.offer_received,
latch.less_than_1_min,
latch.btw_1_to_2_mins,
latch.btw_2_to_3_mins,
latch.abv_3_mins
from
(
select
distinct tbl_dt, start_dt 
from nigeria.clm_provision_details d
order by 1 desc, 2 desc
)ls
left join
(select tbl_dt,count(*) eligibility_count from (
select distinct tbl_dt,MSISDN_KEY MSISDN  from nigeria.segment5b5_sub 
where tbl_dt = $yest and aggr='daily' and dola = 15
and  age_band>2  and exclusion_status ='NA' 
and service_class_id in ('19','25','26','30','31','32','33','39','41','42','43','44','46','48','49','135','185','186')
and tenure is not null) group by tbl_dt
) eli on ls.tbl_dt = eli.tbl_dt
left join
(select
tbl_dt,count(distinct subscriber_id) provisioned_count
from nigeria.clm_provision_details
group by tbl_dt
)prov on ls.tbl_dt = prov.tbl_dt
left join
(select
tbl_dt, count(1) sms_delivery_count
from flare_8.flytxt_latch_dump
where tbl_dt = $yest
group by tbl_dt
)sms on ls.tbl_dt = sms.tbl_dt
left join
(select
tbl_dt,count()success_count
from nigeria.clm_provision_success_file
where is_successful = true
group by tbl_dt
)suc on ls.tbl_dt = suc.tbl_dt
left join
(select
tbl_dt,
count(*)as offer_received,
sum(less_than_1_min)as less_than_1_min,
sum(btw_1_to_2_mins)as btw_1_to_2_mins,
sum(btw_2_to_3_mins)as btw_2_to_3_mins,
sum(abv_3_mins)as abv_3_mins
from
(select
tbl_dt, case when cast(time_diff as integer) < 60 then 1 else 0 end less_than_1_min,
case when cast(time_diff as integer) >= 60 and  cast(time_diff as integer) < 120  then 1 else 0 end btw_1_to_2_mins,
case when cast(time_diff as integer) >= 120 and  cast(time_diff as integer) < 180  then 1 else 0 end btw_2_to_3_mins,
case when cast(time_diff as integer) >= 180 then 1 else 0 end abv_3_mins
from flare_8.flytxt_latch_dump
where tbl_dt = $yest
) a
group by tbl_dt
) latch on ls.tbl_dt = latch.tbl_dt
Where ls.tbl_dt = $yest
order by 1 " --output-format CSV 

echo "Completed for $yest"

##start_date=cast(date_format(date_add('day',+1,date_parse(cast($start_date as varchar), '%Y%m%d')),'%Y%m%d') as INT)

