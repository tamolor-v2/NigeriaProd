#!bin/bash


last_tbl=$(/opt/presto/bin/presto --server 10.1.197.146:9999 --catalog hive5  --schema flare_8 --execute "select count(*) from vw_engine_room_data_rec1")
last_tbl_1=$(echo $last_tbl | sed "s/\"//g")

if [ ! $last_tbl_1 == 0 ]
then
  exit 0
else
 exit 1
fi

