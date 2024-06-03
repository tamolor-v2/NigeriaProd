#!bin/bash

DATE=$1
hdfs dfs -rm  -skipTrash /FlareData/output_8/CELLS/*
echo "started for CELLS for date: $DATE"
  bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed CELLS --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_CELLS_$(date +%Y%m%d_%s).log
 q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.CELLS")
 if [ $q1 != 0 ]
 then 
  echo "finished for for CELLS for date: $DATE successfully"
 else 
  echo "couldn't insert for CELLS, retrying in 10s"
  sleep 10s
  bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed CELLS --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_CELLS_$(date +%Y%m%d_%s).log
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.CELLS")
  if [ $q1 != 0 ]
  then
   echo "finished for for CELLS for date: $DATE successfully"
  else
   echo "couldn't insert for CELLS, Please rerun me!"
   exit 1
  fi
 fi
