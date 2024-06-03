#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$(date -d '-1 day' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
#rm -r /mnt/beegfs_bsl/tools/rand/MTN_LOCATION_REGION/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/nas/share05/tools/Crontab/logs/general_logs
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
##bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 12 --status 0 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/MTN_LOCATION_REGION/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
##echo "Job: extract_MTN_LOCATION_REGION. Status: Started. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
ssh edge01002 " echo -e 'CronJob \"extract_MTN_LOCATION_REGION.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract MTN_LOCATION_REGION Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

bash /nas/share05/tools/ExtractTools/MTN_LOCATION_REGION/driver_MTN_LOCATION_REGION.sh /nas/share05/tools/ExtractTools/MTN_LOCATION_REGION /mnt/beegfs_bsl/FlareData/CDR/MTN_LOCATION_REGION/tmp/ $yest 
retVal=$?
if [ $retVal -eq 0 ];
then
rmdir /mnt/beegfs_bsl/FlareData/CDR/MTN_LOCATION_REGION/incoming/$yest
 mv /mnt/beegfs_bsl/FlareData/CDR/MTN_LOCATION_REGION/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/MTN_LOCATION_REGION/incoming
#Send Job Finished Email
 extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/rand/MTN_LOCATION_REGION/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"extract_MTN_LOCATION_REGION.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract MTN_LOCATION_REGION Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
##echo "Job: extract_MTN_LOCATION_REGION. Status: Finished. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
## bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 12 --status 1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/MTN_LOCATION_REGION/logs/$yest/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
ssh edge01002 " echo -e 'CronJob \"extract_MTN_LOCATION_REGION.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract MTN_LOCATION_REGION Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
##echo "Job: extract_MTN_LOCATION_REGION. Status: Failed. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
##bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 12 --status -1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/MTN_LOCATION_REGION/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi