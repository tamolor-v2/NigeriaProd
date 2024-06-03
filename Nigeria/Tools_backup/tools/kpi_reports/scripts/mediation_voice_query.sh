#!/bin/bash
date=$(date '+%Y%m%d')
old_date=$(date -d '-2 day' '+%Y%m%d')
####/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select cast(hour(current_timestamp) as int) hour , 15 as kpiType_id ,avg(stats_table.diff_land_record)/60 measure_value, 2 as unit_id , 10 as target_id,  date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, ${date} tbl_dt from  (select tbl_dt, filename, date_diff('second',date_parse(date_format(date_parse(substring(split(filename,'/')[7],1,14),'%Y%m%d%H%i%s'),'%Y-%m-%d %H:%i:%s'), '%Y-%m-%d %H:%i:%s'),date_parse(date_format(from_unixtime(fsys_landing_ts/1000),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s')) diff_land_record ,date_format(date_parse(substring(split(filename,'/')[7],1,14),'%Y%m%d%H%i%s'),'%Y-%m-%d %H:%i:%s') file_date, date_format(from_unixtime(fsys_landing_ts/1000),'%Y-%m-%d %H:%i:%s') daas_landing_time from flare_8.file_stats where fsys_msgtype = 'com.mtn.messages.cs5_ccn_voice_ma' and tbl_dt between ${old_date} and ${date} and date_parse(date_format(from_unixtime(fsys_landing_ts/1000),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between date_trunc('hour',(current_timestamp + interval '-1' hour)) and date_trunc('hour',current_timestamp)) stats_table" --output-format CSV


/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "insert into  kpi_reports.network_kpi select cast(hour(current_timestamp) as int) hour , 15 as kpiType_id ,avg(date_diff('second', end_time_ts, date_parse(date_format(from_unixtime(landing_ts),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s')))/60.0 measure_value, 2 as unit_id , 10 as target_id,  date_format(date_trunc('hour',current_timestamp), '%Y-%m-%d %H:%i:%s') current_datetime, ${date} tbl_dt from ( (select original_timestamp_enrich, date_add('second',call_duration, date_parse(date_format(date_parse(original_timestamp_enrich,'%Y%m%d%H%i%s'),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s')) end_time_ts, file_name, tbl_dt from flare_8.cs5_ccn_voice_ma where tbl_dt =${date}  and date_parse(date_format(from_unixtime(cast(kamanja_loaded_date as bigint)),'%Y-%m-%d %H:%i:%s'),'%Y-%m-%d %H:%i:%s') between (current_timestamp + interval '-1' hour) and current_timestamp) a left join (select distinct (fsys_landing_ts/1000) as landing_ts, filename from flare_8.file_stats where  fsys_msgtype = 'com.mtn.messages.cs5_ccn_voice_ma' and tbl_dt between ${old_date} and  ${date}) b on a.file_name = b.filename) x" --output-format CSV
