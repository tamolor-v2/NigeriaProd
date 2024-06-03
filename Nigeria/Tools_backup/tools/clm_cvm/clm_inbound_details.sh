#!/bin/bash

date=$(date '+%Y%m%d')
yest=$(date -d '-1 day' '+%Y%m%d')

echo "Starting for $yest"

/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into nigeria.clm_inbound_details 
select
   cl.msisdn ,
   cl.servicerequestnumber ,
   cl.servicerequestprocessingdate ,
   cl.offercode ,
   cl.offername ,
   cl.offertype ,
   cl.offersubcategory ,
   p.amount,
   cl.action ,
   cl.loginname ,
   cl.username ,
   cl.errormessage ,
   cr.channel,
   cr.retail_shop,
   case when lower(retail_shop) like '%bpo%' then 'Call Center' else 'Outlet' end retail_shop_type,
   cr.msc_lga,cr.msc_city, cr.msc_state,
   cr.summary, cr.status
from
flare_8.clm_wbo cl  
inner join flare_8.call_reason cr on concat('234',cl.msisdn)  = cast(cr.msisdn as varchar)
and date_format(date_parse(try_cast(from_unixtime(servicerequestprocessingdate/1000) as varchar),'%Y-%m-%d %H:%i:%s.%f'),'%Y%m%d') = cast(cr.tbl_dt as varchar) and trim(lower(cr.channel)) = 'phone'
left join (select t1.* from nigeria.cis_catalogue t1,
(select product_id,max(tbl_dt) max_tbl from nigeria.cis_catalogue 
group by product_id) t2
where t1.product_id=t2.product_id and t1.tbl_dt=t2.max_tbl) p on p.product_id = cl.offercode
where cr.tbl_dt = $yest " --output-format CSV 

echo "Completed for $yest"

##start_date=cast(date_format(date_add('day',+1,date_parse(cast($start_date as varchar), '%Y%m%d')),'%Y%m%d') as INT)

