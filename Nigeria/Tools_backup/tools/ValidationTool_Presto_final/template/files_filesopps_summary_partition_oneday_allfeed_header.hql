 insert into audit.files_filesopps_summary_partition_pre
 select distinct name,lines,dup_count,cast(file_dt as int),feed_name from
 (select name,lines,coalesce(b.dup_count,0) dup_count,trim(case
