#!/bin/bash
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
yest=$(date -d '-1 day' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
rm -r /mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/logs/${removeDir}

currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: exctract_PM_RATED. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
#ssh edge01002 " echo -e 'ronJob \"CONNECT_DEACTIVATE.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"

bash /mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/driver.sh /mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE /mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/tmp/Aya $yest 
retVal=$?
if [ $retVal -eq 0 ];
then
mv /mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/CONNECT_DEACTIVATE/incoming
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: exctract_PM_RATED. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/logs/$yest/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
#ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Move PM_RATED to incoming directory Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
else
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract PM_RATED Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CONNECT_DEACTIVATE/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
