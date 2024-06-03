
DATE=$1
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')
counter=0

for feed in CVM20_USG_VOI_USG_CLUSTER_TMP CVM20_VOICE_SMS_INCOMING CVM20_USG_DATA_SMS_VOI_PRD_TMP 
do
 echo "Started for $feed at $SYS_DATE"
 hdfs dfs -rm -skipTrash /FlareData/CVM_DB/${feed}/tbl_dt=${DATE}/*
 if [ $feed == "CVM20_USG_DATA_SMS_VOI_PRD_TMP" ]
 then
  python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from $feed where tbl_dt=${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
 else
  python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from $feed")
  q11=$(echo $q1 |  sed "s/\"//g")
 fi
 if [ $q11 != 0 ]
 then
    echo "Finished successfully for date $DATE for $feed table at $SYS_DATE with $q11 inserted records!"
  if [ $feed == "CVM20_USG_DATA_SMS_VOI_PRD_TMP" ]
  then 
     echo -e "Process done successfully! \n $feed table count: $q11"
     mkdir -p /nas/share05/tools/CVM_Reports/logs/$DATE
  else
     echo "Moving to the next table on the queue.. "
  fi 
 else
    echo "Failed to insert into $feed , It will affect the inserting process!"
    counter=$((counter+1))
 fi
done

## checking if all tables has data..
if [ $counter == 0 ]
then
  echo -e "Area table $feed filled with $q11 records!\nJob Succeeded!"
else
  echo -e "Error: One of the base tables -or the Area table- failed to be inserted! Check the above log to see which one.\nJob Failed!"
  exit 1;
fi
