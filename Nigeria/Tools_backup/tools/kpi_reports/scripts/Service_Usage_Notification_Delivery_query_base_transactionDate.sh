#!/bin/bash
date=$(date '+%Y%m%d')
old_date=$(date -d '-10 day' '+%Y%m%d')
/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select cast(hour(date_parse(original_timestamp_enrich,'%Y-%m-%d %H:%i:%s')) as int) hour , 9 as kpiType_id, sum(case when error_code = 0 or error_code in (select delivererror from kpi_reports.dim_subscribers_and_network_errors) then count else 0 end)/cast (sum(count) as double) * 100  measure_value , 4 as unit_id , 9 as target_id,  date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, tbl_dt from flare_8.smsc_total_delivery_success_rate where OrgAccount = 'cssdpMTN' and tbl_dt between ${old_date} and ${date} and date_parse(date_format(from_unixtime(cast(kamanja_loaded_date as bigint)),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between date_trunc('hour',(current_timestamp + interval '-1' hour)) and date_trunc('hour',current_timestamp) group by tbl_dt, cast(hour(date_parse(original_timestamp_enrich,'%Y-%m-%d %H:%i:%s')) as int)" --output-format CSV