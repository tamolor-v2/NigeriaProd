
DATE=$1
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')
counter=0

for feed in CVM20_REFILL_AND_DATA_BUNDLE_TMP1 CVM20_RBS_AVERAGE_RCH_GAP_TMP2 CVM20_RBS_AVERAGE_SBSC_GAP_VOICE_TMP1 CVM20_RBS_AVERAGE_SBSC_GAP_DATA_TMP1 CVM20_RBS_AVERAGE_SBSC_GAP_SMS_TMP1 CVM20_RBS_AVERAGE_SBSC_GAP_COMBO_TMP1 CVM20_RBS_AVERAGE_SBSC_GAP_VAS_TMP1 CVM20_RBS_AVERAGE_SBSC_GAP_TMP1 CVM20_REFILL_DATA_BUNDLE
do
 echo "Started for $feed at $SYS_DATE"
 hdfs dfs -rm -skipTrash /FlareData/CVM_DB/${feed}/tbl_dt=${DATE}/*
 if [ $feed != "CVM20_REFILL_DATA_BUNDLE" ]
 then
  python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from $feed where tbl_dt=${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
 else
  python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from cvm20_refill_and_data_bundle_uat_tmp2")
  q11=$(echo $q1 |  sed "s/\"//g")
 fi
 if [ $q11 != 0 ]
 then
    echo "Finished successfully for date $DATE for $feed table at $SYS_DATE with $q11 inserted records!"
  if [ $feed == "CVM20_REFILL_DATA_BUNDLE" ]
  then 
     echo -e "Process done successfully! \n CVM20_REFILL_AND_DATA_BUNDLE_UAT_TMP2 table count: $q11"
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
