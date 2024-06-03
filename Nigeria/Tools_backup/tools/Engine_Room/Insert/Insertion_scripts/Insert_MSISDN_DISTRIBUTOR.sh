#!bin/bash

DATE=$(date -d '-1 day' '+%Y%m%d')
DATE3=$(date -d '-4 day' '+%Y%m%d')
Feed="MSISDN_DISTRIBUTOR"
BaseFeed="ers_vend_new"
echo "started for ${Feed} for date: $DATE"

check=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from (select case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER')  then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end as transaction_type from flare_8.ERS_VEND_NEW where senderresellertype = 'hostifdistributor' and channel = 'USSD' and tbl_dt=${DATE}) where transaction_type in ('SELL_IN')")
check1=$(echo $check |  sed "s/\"//g")
if [ $check1 != 0 ]
then
 bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_${Feed}_$(date +%Y%m%d_%s).log
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --execute "select count(*) from engine_room.${Feed} where tbl_dt=${DATE}") 
  q11=$(echo $q1 |  sed "s/\"//g")
  res=$(echo $q11)
   if [ $res != 0 ]
   then
    echo "Finished for ${Feed} for date: $DATE successfully with $q11 Inserted records"
   else
    echo "Couldn't Insert the data for ${Feed}, Retrying one more time in 30 sec"
    sleep 30s

    bash  /nas/share05/tools/Engine_Room/Insertion/Insertion.scala --configFile /nas/share05/tools/Engine_Room/Insertion/Insertion.conf --feed ${Feed} --dates $DATE 2>&1 | tee /nas/share05/tools/Engine_Room/Insertion/logs/Insert_${Feed}_$(date +%Y%m%d_%s).log
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
else
echo "The table hasn't data to insert"
fi
  echo "=================================="
