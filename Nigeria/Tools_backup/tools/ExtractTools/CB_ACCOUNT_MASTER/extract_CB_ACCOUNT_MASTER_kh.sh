#!/bin/bash
#working_folder=$1
#incoming_folder=$2
#extract_folder="${working_folder}/tmp"

yest=$1
#emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
#removeDir=$(date -d '-60 day' '+%Y%m%d')
#rm -r /mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs_bsl/tools/Crontab/logs/general_logs
#Send Start Job Email
#rm -rf  /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp/$yest
#ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
#kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
#TopicName=CentralMetaStore
#hostName=$(hostname)
#bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 101 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here

#ssh edge01002 " echo -e 'CronJob \"extract_CB_ACCOUNT_MASTER.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CB_ACCOUNT_MASTER started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: CB_ACCOUNT_MASTER. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

bash /nas/share05/tools/ExtractTools/CB_ACCOUNT_MASTER/driver_CB_ACCOUNT_MASTER.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/ /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp $yest "0 1 2 3 4"

bash /nas/share05/tools/ExtractTools/CB_ACCOUNT_MASTER/driver_CB_ACCOUNT_MASTER.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/ /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp $yest "5 6 7 8 9"

#retVal=$?
#if [ $retVal -eq 0 ];
#then
#Send Job Finished Email
#mkdir /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp/${yest}_tmp/

#mv /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp/${yest}/*/* /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp/${yest}_tmp/

#bash /mnt/beegfs/Deployment/DEV/scripts/splittingScirpts/SplitCbAccountMaster.sh --landing_dir "/mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp/${yest}_tmp/" --working_dir /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/workingDir/ --kamanja_incoming_dir /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/splitted/

#mv /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/splitted/* /mnt/beegfs_bsl/FlareDataTest/CB_ACCOUNT_MASTER/incoming/
#echo "Moving /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/tmp/$yest /mnt/beegfs_bsl/FlareData/CDR/CB_ACCOUNT_MASTER/incoming/" 

mv /mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/CB_ACCOUNT_MASTER/incoming

#extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"exctract_CB_ACCOUNT_MASTER\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_< extract CB_ACCOUNT_MASTER Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: CB_ACCOUNT_MASTER. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 101 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/logs/$yest/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
#else
#ssh edge01002 " echo -e 'CronJob \"exctract_CB_ACCOUNT_MASTER\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract CB_ACCOUNT_MASTER Fialed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#echo "Job: CB_ACCOUNT_MASTER. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 101 --status -1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs_bsl/tools/ExtractTools/CB_ACCOUNT_MASTER/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
#fi
