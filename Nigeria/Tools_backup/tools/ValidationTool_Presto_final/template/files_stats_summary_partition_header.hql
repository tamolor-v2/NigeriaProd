insert into audit.files_count_stats_summary
SELECT path, split(path,'/')[cardinality(split(path,'/'))] , processedrecordscount, recordscount, status, coalesce(cast(CASE
