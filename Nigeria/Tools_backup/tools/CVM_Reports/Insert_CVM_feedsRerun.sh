

#This one to run for the user input feeds
#Takes the date and the list of feeds 
#i.e --> bash script.sh 20210501 "x" "y" "z" 

DATE=$1
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')

read -a arr <<< $2

for feed in ${arr[@]}
do
 echo "Started for $feed at $SYS_DATE"
  python3.6  /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfig.json -qn $feed -rd $DATE
  #python /nas/share05/tools/Engine_Room/runPrestoFile/bin/run_presto.py -s $DATE -q $feed
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema cvm_db --execute "select count(*) from $feed where tbl_dt=${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
 if [ $q11 != 0 ]
 then
    echo "Finished successfully for date $DATE for $feed table at $SYS_DATE with $q11 inserted records!"
  if [ $feed == ${arr[-1]} ]
  then 
     echo -e "Process done successfully! \n $feed table count: $q11"
  else
     echo "Moving to the next table on the queue.. "
  fi 
 else
    echo "Failed to insert into $feed , It will affect the inserting process!"
 fi
done
