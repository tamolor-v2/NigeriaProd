#!/bin/bash
today=$1
yest=$(date -d "$1 -1 day" '+%Y%m%d')
start_dt=$(date -d "$1 -8 day" '+%Y%m%d')
end_dt=$(date -d "$1 -2 day" '+%Y%m%d')


result=$("/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format CSV_HEADER select count(*) from flare_8.customersubject_dashboard where tbl_dt=${today};")

echo "$result"
