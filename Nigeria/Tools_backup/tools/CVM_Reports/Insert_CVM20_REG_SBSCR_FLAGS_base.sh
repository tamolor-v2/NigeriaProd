


DATE=$1
DATE_PREV=$(($DATE -1))
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')

for feed in CVM20_REG_SBSCR_BASE CVM20_REG_SBSCR_FLAGS 
do
 echo "Started for $feed at $SYS_DATE"
 hdfs dfs -rm -skipTrash /FlareData/CVM_DB/${feed}/tbl_dt=${DATE}/*
 python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from CVM20_REG_SBSCR_FLAGS where tbl_dt=${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
  q2=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from CVM20_REG_SBSCR_BASE where tbl_dt=${DATE_PREV}")
  q22=$(echo $q2 |  sed "s/\"//g")
 if [ $q22 != 0 ]
 then
    echo "Finished successfully for date $DATE for $feed table at $SYS_DATE with $q22 inserted records!"
  if [ $feed == "CVM20_REG_SBSCR_FLAGS" ]
  then 
     if [ $q11 != 0 ]
     then
        echo -e "Process done successfully! \n $feed table count: $q11"
        mkdir -p /nas/share05/tools/CVM_Reports/logs/$DATE
     else
       echo "Failed to insert into $feed , It will affect the inserting process!"
       exit 1
     fi
  else
     echo "Moving to the next table on the queue.. "
  fi 
 else
    echo "Failed to insert into $feed , It will affect the inserting process!"
    exit 1
 fi
done

