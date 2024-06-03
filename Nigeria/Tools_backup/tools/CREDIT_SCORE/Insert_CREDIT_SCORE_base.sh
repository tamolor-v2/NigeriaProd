
DATE=$1
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')

 echo "Started for CR_BAL_EOD at $SYS_DATE"
  hdfs dfs -rm -skipTrash /FlareData/output_8/CR_BAL_EOD/*
  hdfs dfs -rm -skipTrash /FlareData/output_8/CREDIT_SCORE_FINAL/tbl_dt=${DATE}/*
  python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn CR_BAL_EOD -rd $DATE
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema flare_8 --execute "select count(*) from CR_BAL_EOD")
  q11=$(echo $q1 |  sed "s/\"//g")
 if [ $q11 != 0 ]
 then
     echo "Finished successfully for date $DATE for CR_BAL_EOD table at $SYS_DATE with $q11 inserted records!"
     python3.6 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn CREDIT_SCORE_FINAL -rd $DATE
     q2=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema flare_8 --execute "select count(*) from CREDIT_SCORE_FINAL where tbl_dt=${DATE}")
     q22=$(echo $q2 |  sed "s/\"//g")
     if [ $q22 != 0 ]
     then
         echo -e "Process done successfully! \n CREDIT_SCORE_FINAL table count: $q22"
     else
         echo "Failed to insert into CREDIT_SCORE_FINAL , It will affect the inserting process!"
     fi
 else
    echo "Failed to insert into CR_BAL_EOD , It will affect the inserting process!"
    exit 1
 fi
