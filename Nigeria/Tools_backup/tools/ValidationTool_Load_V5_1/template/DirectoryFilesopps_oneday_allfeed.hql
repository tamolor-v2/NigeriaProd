insert into databaseTables.directory_filesopps_summary_pre select feed_name,sum(lines),count(*),file_dt from databaseTables.files_filesopps_summary_partition_pre where file_dt=targetdate group by feed_name,file_dt;