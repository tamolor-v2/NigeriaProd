#!/bin/bash
curr_date=$(date -d '-1 hour' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
#rm -r /mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
#ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
#kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
#TopicName=CentralMetaStore
#hostName=$(hostname)
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 8 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change/logs/$curr_date/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
echo "Job: extract_CHG_Infrastructure_Change. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${curr_date}.log

#Send Start Job Email
#ssh edge01002 "  echo -e 'CronJob \"extract_CHG_Infrastructure_Change.sh\" Started at $(date +"%T") on edge01001, for day: $curr_date \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CHG_Infrastructure_Change Started for $curr_date at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

bash /nas/share05/tools/ExtractTools/CHG_Infrastructure_Change/driver_CHG_Infrastructure_Change.sh /mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change /mnt/beegfs/FlareData/CDR/CHG_Infrastructure_Change/tmp/ $curr_date 
retVal=$?
if [ $retVal -eq 0 ];
then
 mv /mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/CHG_Infrastructure_Change/incoming
#Send Job Finished Email
 extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change/logs/$curr_date/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"extract_CHG_Infrastructure_Change.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $curr_date \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CHG_Infrastructure_Change Finished for $curr_date with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
#echo "Job: extract_CHG_Infrastructure_Change. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 8 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change/logs/$curr_date/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
echo "failed"
#ssh edge01002 " echo -e 'CronJob \"extract_CHG_Infrastructure_Change.sh\" Failed at $(date +"%T"), for day: $curr_date \n' | mailx -r 'DAAS_Alert_NG@edge01002.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract CALL_REASON Failed for $curr_date at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
#echo "Job: extract_CHG_Infrastructure_Change. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 8 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CHG_Infrastructure_Change/logs/$curr_date/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
