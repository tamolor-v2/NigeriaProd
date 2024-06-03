
DATE=$1
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')
emailReceiver=$(cat  /nas/share05/tools/Crontab/Scripts/email.dat)
counter=0

for feed in CVM20_CUSTOMERSUBJECT_SN_TMP CVM20_USG_DATA_SMS_VOI_TMP 
do
 echo "Started for $feed at $SYS_DATE"
 hdfs dfs -rm -skipTrash /FlareData/CVM_DB/${feed}/tbl_dt=${DATE}/*
  python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from $feed where tbl_dt=${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
 if [ $q11 != 0 ]
 then
    echo "Finished successfully for date $DATE for $feed table at $SYS_DATE with $q11 inserted records!"
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
