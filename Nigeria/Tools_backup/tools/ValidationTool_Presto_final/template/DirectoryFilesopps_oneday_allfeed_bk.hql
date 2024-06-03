insert into audit.directory_filesopps_summary select feed_name,sum(lines),count(*),file_dt from audit.files_filesopps_summary_partition_pre where file_dt=targetdate group by feed_name,file_dt;
