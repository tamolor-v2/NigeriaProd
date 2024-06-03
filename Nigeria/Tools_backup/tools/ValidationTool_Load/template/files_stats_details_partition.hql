set hive.exec.dynamic.partition.mode=nonstrict;
set tez.am.resource.memory.mb = 3048;
set hive.tez.container.size=32096;
set tez.runtime.io.sort.mb = 4090;
insert overwrite table databasename.files_count_stats_detailes  partition (tbl_dt,feed_name)   
 select 
 row_number() over(partition by filename order by starttime desc) seq, 
 filename, 
 status,
 starttime,
 recordscount,
 processedrecordscount,
 tbl_dt,
 upper(substring(fsys_msgtype,18)) feed_name
 from databasename.file_stats where tbl_dt>=predate ;
