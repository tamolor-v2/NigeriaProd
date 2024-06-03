#!bin/bash

DATE=$(date -d '-1 day' '+%Y%m%d')
DATE3=$(date -d '-4 day' '+%Y%m%d')
Feed="MFS_ACTIVE_SUBSCRIBERS"
BaseFeed="ewp_account_holders_dump"
echo "started for ${Feed} for date: $DATE"

  bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_${Feed}_$(date +%Y%m%d_%s).log

  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.${Feed} where tbl_dt=${DATE}") 
  q2=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "select count(*) from ${BaseFeed} where tbl_dt=${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
  q22=$(echo $q2 |  sed "s/\"//g")
  q=$((q11-q22))
  res=$(echo $q)
   if [ $res == 0 ]
   then
    echo "Finished for ${Feed} for date: $DATE successfully with $q11 Inserted records"
   else
    echo "Couldn't Insert the data for ${Feed}, Retrying one more time in 30 sec"
    sleep 30s

    bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_${Feed}_$(date +%Y%m%d_%s).log
	q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.${Feed} where tbl_dt=${DATE}")
	q11=$(echo $q1 |  sed "s/\"//g")
	q=$((q11-q22))
	res=$(echo $q)
	if [ $res == 0 ]
	then 
         echo "Finished for ${Feed} for date: $DATE successfully with $q11 Inserted records"
	else
         echo "Couldn't Insert the data for ${Feed} due to Presto issue or mismatching numbers, Please rerun me! "	
	 hdfs dfs -rm -skipTrash /FlareData/output_8/${Feed}/tbl_dt=${DATE}/*
	 exit 1
	fi 
   fi
  echo "=================================="
