ELSE    "19700101"    END as int ),19700101)  file_dt,
tbl_dt,
feed_name
from databasename.files_count_stats_detailes where tbl_dt>=predate and seq=1;

