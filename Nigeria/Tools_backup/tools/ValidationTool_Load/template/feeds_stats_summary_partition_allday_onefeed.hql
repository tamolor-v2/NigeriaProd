set hive.exec.dynamic.partition.mode=nonstrict;
set tez.am.resource.memory.mb = 3048;
set hive.tez.container.size=32096;
set tez.runtime.io.sort.mb = 4090;
insert overwrite table databasename.feeds_count_stats_summary partition (file_dt)           
 select
 count(*) as numberoffiles, 
 sum(recordscount) as recordscount,
 sum(processedrecordscount) as processedByKamanja, 
 feedname,
 file_dt     
 from databasename.files_count_stats_summary where file_dt= targetdate
 group by feedname,file_dt;
