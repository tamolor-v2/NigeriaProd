#!/bin/bash
date=$(date '+%Y%m%d')
old_date=$(date -d '-2 day' '+%Y%m%d')
###hive -e 'msck repair table kpi_reports.mymtnapp'

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select cast(hour(current_timestamp) as int) hour , 7 as kpiType_id , avg(cast (regexp_extract(regexp_extract(column_17, 'Cost time : \d+ ms.'), '\d+') as double)/1000) measure_value, 1 as unit_id , 5 as target_id, date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, ${date} tbl_dt from flare_8.sdp_api where api_name = 'sendSms' and response_request = 'response' and tbl_dt between ${old_date} and ${date} and date_parse(date_format(from_unixtime(cast(kamanja_loaded_date as bigint)),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between date_trunc('hour',(current_timestamp + interval '-1' hour)) and date_trunc('hour',current_timestamp)" --output-format CSV
