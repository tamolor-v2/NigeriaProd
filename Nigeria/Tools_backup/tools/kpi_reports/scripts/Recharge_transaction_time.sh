#!/bin/bash
date=$(date '+%Y%m%d')
old_date=$(date -d '-2 day' '+%Y%m%d')
/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select cast (hour(current_timestamp) as int) hour , 11 as kpiType_id , avg( date_diff('second',adj.timestamp_V,refill.timestamp_V)) measure_value, 1 as unit_id , 5 as target_id,  date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, ${date}  tbl_dt from (select msisdn_key,transactionamount, origintransactionid, origintimestamp, original_timestamp_enrich, date_parse(original_timestamp_enrich,'%Y%m%d%H%i%s') timestamp_V from flare_8.cs5_air_refill_ma where tbl_dt between ${old_date} and ${date} and date_parse(date_format(from_unixtime(cast(kamanja_loaded_date as bigint)),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between date_trunc('hour',(current_timestamp + interval '-1' hour)) and date_trunc('hour',current_timestamp) ) refill inner join (select msisdn_key,adjustmentamount, origtransactionid, origtransactiontimestamp, original_timestamp_enrich, date_parse(original_timestamp_enrich,'%Y%m%d%H%i%s') timestamp_V from flare_8.cs5_sdp_acc_adj_ma where tbl_dt between ${old_date} and ${date} and date_parse(date_format(from_unixtime(cast(kamanja_loaded_date as bigint)),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between (date_trunc('hour',current_timestamp + interval '-1' hour)) and date_trunc('hour',current_timestamp) ) adj on refill.msisdn_key = adj.msisdn_key and refill.transactionamount = adj.adjustmentamount and refill.origintransactionid = adj.origtransactionid" --output-format CSV
