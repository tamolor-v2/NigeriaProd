insert into databaseTables.files_count_stats_summary_pre
SELECT path, split(path,'/')[cardinality(split(path,'/'))] , processedrecordscount, recordscount, status, coalesce(cast(CASE
