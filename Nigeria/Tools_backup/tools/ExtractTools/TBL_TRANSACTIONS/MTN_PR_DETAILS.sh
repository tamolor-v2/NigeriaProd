#!/bin/bash
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/logs/$curr_date/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
curr_date=$(date -d '-1 hour' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
rm -r /mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/logs/${curr_date}

currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")


#Send Start Job Email
#ssh edge01002 " echo -e 'ronJob \"MTN_PR_DETAILS.sh\" Started at $(date +"%T") on edge01001, for day: $curr_date \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Started for $curr_date at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
bash /mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/driver.sh /mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS /mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/tmp $curr_date
retVal=$?
if [ $retVal -eq 0 ];
then
mv /mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/tmp/$curr_date /mnt/beegfs/FlareDataTest/MTN_PR_DETAILS/incoming/
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
extractedRecordsNo=$(cat /mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/logs/$curr_date/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $curr_date \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Finished for $curr_date with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: exctract_PM_RATED. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/logs/$curr_date/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
echo "hi"
#ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $curr_date \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Move PM_RATED to incoming directory Failed for $curr_date at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/logs/$curr_date/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
else
echo "hi"
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $curr_date \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract PM_RATED Failed for $curr_date at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/ExtractTools/MTN_PR_DETAILS/logs/$curr_date/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi