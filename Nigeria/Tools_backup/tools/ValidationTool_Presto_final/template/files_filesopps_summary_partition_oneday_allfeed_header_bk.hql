 insert into audit.files_filesopps_summary_partition_pre
 select distinct name,lines,cast(file_dt as int),feed_name from
 (select name,lines,trim(case
