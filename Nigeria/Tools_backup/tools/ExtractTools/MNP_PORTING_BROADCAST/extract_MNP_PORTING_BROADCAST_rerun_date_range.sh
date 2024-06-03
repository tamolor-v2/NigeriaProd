#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

date_run=$1
week7=$(date -d "${date_run} -7 day" '+%Y%m%d')
week6=$(date -d "${date_run} -6 day" '+%Y%m%d')
week5=$(date -d "${date_run} -5 day" '+%Y%m%d')
week4=$(date -d "${date_run} -4 day" '+%Y%m%d')
week3=$(date -d "${date_run} -3 day" '+%Y%m%d')
week2=$(date -d "${date_run} -2 day" '+%Y%m%d')
yest=$(date -d "${date_run} -1 day" '+%Y%m%d')

echo $week7
echo $week6
echo $week5
echo $week4
echo $week3
echo $week2
echo $yest
#exit;
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
removeDir=$(date -d '-60 day' '+%Y%m%d')
rm -r /mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs_bsl/tools/Crontab/logs/general_logs
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 4 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here

echo "Job: extract_MNP_PORTING_BROADCAST. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
ssh edge01002 " echo -e 'CronJob \"extract_MNP_PORTING_BROADCAST.sh\" Started at $(date +"%T") on edge01001, from day: $week7 until $yest\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract MNP_PORTING_BROADCAST Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
########
#######
bash /mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/driver_MNP_PORTING_BROADCAST.sh /mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST /mnt/beegfs_bsl/FlareData/CDR/MNP_PORTING_BROADCAST/tmp/ $week7 $yest
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
if [ "$extractedRecordsNo" > 0 ]; 
then 
 kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
#  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week7
#  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week6
#  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week5
#  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week4
#  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week3
#  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week2
  hdfs dfs -rm /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$yest/*
  rmdir /mnt/beegfs_bsl/live/DB_extract_lz/MNP_PORTING_BROADCAST/incoming/$yest
  rmdir /mnt/beegfs_bsl/FlareData/CDR/MNP_PORTING_BROADCAST/incoming/$yest

  mv /mnt/beegfs_bsl/FlareData/CDR/MNP_PORTING_BROADCAST/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/MNP_PORTING_BROADCAST/incoming/
fi
ssh edge01002 " echo -e 'CronJob \"exctract_MNP_PORTING_BROADCAST.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, from day: $week7 until $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract MNP_PORTING_BROADCAST Finished from $week7 to  $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: extract_MNP_PORTING_BROADCAST. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 4 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/logs/$yest/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
ssh edge01002 " echo -e 'CronJob \"exctract_MNP_PORTING_BROADCAST.sh\" Failed at $(date +"%T"), from day: $week7 until $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract MNP_PORTING_BROADCAST Failed from $week7 to  $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: extract_MNP_PORTING_BROADCAST. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 4 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
