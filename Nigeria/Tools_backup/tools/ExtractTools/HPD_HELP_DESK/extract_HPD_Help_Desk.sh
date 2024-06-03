#!/bin/bash
yest=$(date -d '-2 day' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
#rm -r /mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
#ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
#kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
#TopicName=CentralMetaStore
#hostName=$(hostname)
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 8 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
echo "Job: extract_HPD_Help_Desk. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
#ssh edge01002 "  echo -e 'CronJob \"extract_HPD_Help_Desk.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract HPD_Help_Desk Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

bash /nas/share05/tools/ExtractTools/HPD_Help_Desk/driver_HPD_Help_Desk.sh /mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk /mnt/beegfs/FlareData/CDR/HPD_Help_Desk/tmp/ $yest 
retVal=$?
if [ $retVal -eq 0 ];
then
 mv /mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/HPD_Help_Desk/incoming
#Send Job Finished Email
 extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"extract_HPD_Help_Desk.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract HPD_Help_Desk Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
#echo "Job: extract_HPD_Help_Desk. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 8 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk/logs/$yest/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
echo "failed"
#ssh edge01002 " echo -e 'CronJob \"extract_HPD_Help_Desk.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01002.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract CALL_REASON Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
#echo "Job: extract_HPD_Help_Desk. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 8 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/HPD_Help_Desk/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
