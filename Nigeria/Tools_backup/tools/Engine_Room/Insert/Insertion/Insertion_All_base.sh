#!bin/bash

DATE=$1

declare -a List=(
"EVD_STOCK_LEVEL"
"EVD_TRANSACTIONS"
"MFS_ACCOUNTS"
"MFS_ACQUISITION"
"MFS_REGISTRATIONS"
"MFS_STOCK_LEVEL"
"SIM_TRANSACTIONS"
"MFS_ACTIVE_SUBSCRIBERS"
"CDR_DATA"
"CDR_RECHARGE"
"CDR_SUBSCRIPTION"
"CDR_SMS"
"CDR_VOICE"
"MSISDN_DISTRIBUTOR"
"POS_MSISDN"
"POS_GEO_PLACES"
"PHYSICAL_RECHARGES"
"MFS_TRANSACTIONS"
"BUNDLE_SPLIT"
)

declare -a num=(0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18)

hdfs dfs -rm  -skipTrash /FlareData/output_8/CELLS/*

for var in ${num[@]} 
 do
  echo "started for ${List[$var]} for date: $DATE"
  bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${List[$var]} --dates $DATE 
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.${List[$var]} where tbl_dt=${DATE}")  
  q11=$(echo $q1 |  sed "s/\"//g")
  res=$(echo $q11)
   if [ $res == 0 ] 
   then 
    echo "Inserted $res records for ${List[$var]} for date: $DATE successfully"
   else
    echo "Couldn't Insert the data for ${List[$var]}"
   fi
  echo "=================================="
done
