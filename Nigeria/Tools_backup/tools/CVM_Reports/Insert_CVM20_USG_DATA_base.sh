
DATE=$1
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')

for feed in CVM20_CUSTOMERSUBJECT_SN_TMP CVM20_USG_DATA_SMS_VOI_TMP CVM20_USG_DATA
do
 echo "Started for $feed at $SYS_DATE"
 if [ $feed == "CVM20_USG_DATA" ]
 then
  hdfs dfs -rm -skipTrash /FlareData/CVM_DB/${feed}/tbl_dt=${DATE}/*
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
  if [ $feed == "CVM20_USG_DATA" ]
  then 
     echo -e "Process done successfully! \n $feed table count: $q11"
  else
     echo "Moving to the next table on the queue.. "
  fi 
 else
    echo "Failed to insert into $feed , It will affect the inserting process!"
 fi
done
