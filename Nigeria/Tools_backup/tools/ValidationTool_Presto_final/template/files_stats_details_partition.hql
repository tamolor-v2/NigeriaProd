insert into audit.files_count_stats_detailes
select row_number() over(partition by filename order by starttime desc) seq, filename, status, starttime, recordscount, processedrecordscount, tbl_dt,
upper(substring(fsys_msgtype,18)) feed_name from audit.file_stats where tbl_dt>=predate;
