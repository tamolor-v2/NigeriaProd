#!/bin/bash
date=$(date '+%Y%m%d')
old_date=$(date -d '-2 day' '+%Y%m%d')
###hive -e 'msck repair table kpi_reports.mymtnapp'

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select cast(hour(current_timestamp) as int) hour , 10 as kpiType_id , avg(date_diff('millisecond',date_parse(original_timestamp_enrich,'%Y-%m-%d %H:%i:%s.%f'),date_parse(resp_time,'%Y-%m-%d %H:%i:%s.%f')))/1000 measure_value, 1 as unit_id , 5 as target_id, date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, ${date} tbl_dt from flare_8.mymtnapp where  (category = 'myAccount' and method in ('balance', 'subscriptionList', 'doVoucherTopup','GetCustomerProfile','lastRecharge','GetCustomerProfile') or (category = 'bundles' and method = 'subscribe') or (category = 'myAcount' and method = 'GetCustomerProfile') ) and tbl_dt between ${old_date} and ${date} and date_parse(date_format(from_unixtime(cast(kamanja_loaded_date as bigint)),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between date_trunc('hour',(current_timestamp + interval '-1' hour)) and date_trunc('hour',current_timestamp)" --output-format CSV
