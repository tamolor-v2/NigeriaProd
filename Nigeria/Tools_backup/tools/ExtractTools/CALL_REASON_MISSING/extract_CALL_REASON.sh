#!/bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
rm -r /mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 3 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/$yest" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
echo "Job: exctract_CALL_REASON. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
ssh edge01002 " echo -e 'CronJob \"extract_CALL_REASON.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CALL_REASON Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

bash /mnt/beegfs/tools/ExtractTools/CALL_REASON/driver_CALL_REASON.sh /mnt/beegfs/tools/ExtractTools/CALL_REASON /mnt/beegfs/FlareData/CDR/CALL_REASON/tmp/ $yest "0 1 2 3 4 5 6 7 8 9"
retVal=$?
if [ $retVal -eq 0 ];
then
mv /mnt/beegfs/FlareData/CDR/CALL_REASON/tmp/$yest /mnt/beegfs/FlareData/CDR/CALL_REASON/incoming/
#Send Job Finished Email
extractedRecordsNo=$(cat /mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"exctract_CALL_REASON.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CALL_REASON Finished with $extractedRecordsNo extracted records for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_CALL_REASON. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 3 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/$yest" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
ssh edge01002 " echo -e 'CronJob \"exctract_CALL_REASON.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract CALL_REASON Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: exctract_CALL_REASON. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 3 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/$yest" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
