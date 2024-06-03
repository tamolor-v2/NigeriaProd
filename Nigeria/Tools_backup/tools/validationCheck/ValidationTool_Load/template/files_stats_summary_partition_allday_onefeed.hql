set hive.exec.dynamic.partition.mode=nonstrict;
set tez.am.resource.memory.mb = 3048;
set hive.tez.container.size=32096;
set tez.runtime.io.sort.mb = 4090;
insert overwrite table databasename.files_count_stats_summary partition (file_dt, tbl_dt,feedname)
SELECT path,
split(path,"\/")[size(split(path,"\/"))-1] ,
processedrecordscount,
recordscount,
status,
'FeedFileterValues',
tbl_dt,
feed_name
from databasename.files_count_stats_detailes where seq=1 and feed_name = 'FeedNameValues';

