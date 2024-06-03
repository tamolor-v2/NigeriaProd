#!/bin/bash
date=$(date '+%Y%m%d')
date_1=$(date -d '-2 day' '+%Y%m%d')
###hive -e 'msck repair table kpi_reports.mymtnapp'

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select 98 hour , 22 as kpiType_id , sum(case when BILLING_SDP_VARIANCE = '0' or BILLING_SDP_VARIANCE = '0.0' or BILLING_SDP_VARIANCE = 'null' then 1 else 0 end)/cast (count(*) as double) * 100 measure_value, 4 as unit_id , 14 as target_id, date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, ${date_1} tbl_dt from flare_8.AGL_BILL_ANALYZER_MONTHLY  where  tbl_dt = ${date}" --output-format CSV
