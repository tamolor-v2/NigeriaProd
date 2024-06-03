#!bin/bash

DATE=$(date -d '-2 day' '+%Y%m%d')
DATE3=$((DATE-3))
Feed="MFS_ACCOUNTS"
echo "started for ${Feed} for date: $DATE"
 

isEmpty=$(hadoop fs -ls hdfs://ngdaas/FlareData/output_8/${Feed}/* | grep ${DATE} | awk '{print $5}' | wc -l )
if [[ $isEmpty -eq 0 ]]
then
  bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_yest_${Feed}_$(date +%Y%m%d_%s).log
else
  hdfs dfs -rm -skipTrash /FlareData/output_8/${Feed}/tbl_dt=${DATE}/*
  bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_yest_${Feed}_$(date +%Y%m%d_%s).log
fi

  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.${Feed} where tbl_dt=${DATE}") 
  q11=$(echo $q1 |  sed "s/\"//g")
  res=$(echo $q11)
   if [ $res != 0 ]
   then
    echo "Finished for ${Feed} for date: $DATE successfully with $q11 Inserted records"
   else
    echo "Couldn't Insert the data for ${Feed}, Retrying one more time in 30 sec"
    sleep 30s

    bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_yest_${Feed}_$(date +%Y%m%d_%s).log
	q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.${Feed} where tbl_dt=${DATE}")
	q11=$(echo $q1 |  sed "s/\"//g")
	res=$(echo $q11)
	if [ $res != 0 ]
	then 
         echo "Finished for ${Feed} for date: $DATE successfully with $q11 Inserted records"
	else
         echo "Couldn't Insert the data for ${Feed} due to Presto issue or mismatching numbers, Please rerun me! "	
	 hdfs dfs -rm -skipTrash /FlareData/output_8/${Feed}/tbl_dt=${DATE}/*
	 exit 1
	fi 
   fi
  echo "=================================="
