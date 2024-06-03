#!/bin/bash
if [ $# -eq 0 ]
  then
    echo "No arguments supplied, provide date in format yyyymmdd"
exit
fi
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
#bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
yest=$(date -d "$1 -10 day" '+%Y%m%d')
cd /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /nas/share05/tools/Crontab/Scripts/email.dat)
#rm -r /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/${removeDir}
#mkdir -p /nas/share05/tools/Crontab/logs/general_logs/${yest}
#runDate=$(date +"%Y%m%d")
runTime=$(date +"%H%M%S")

#if [[ "$runTime" < "235959" ]]
#then 
#currDate=$yest
#echo "Running for yesterday"
#else 
echo "running for $1"
prevMaxSeq="${yest}000000"
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
currdate=$(date +"%Y%m%d")
echo "currdate=$currdate"
generalLogs=/nas/share05/tools/Crontab/logs/general_logs
currMaxSeq="$(date -d "$1 +10 day" '+%Y%m%d')000000"
maxSeq="${yest}000000"
echo "max_seq=$maxSeq"
echo "currMax_seq=$currMaxSeq"
hadoop fs -mkdir -p /FlareData/output8_backup/WBS_PM_RATED_CDRS/tbl_dt=${1}/${currdate}${runTime}
echo "moving partition to /FlareData/output8_backup/WBS_PM_RATED_CDRS/tbl_dt=${1}/${currdate}${runTime}"
hadoop fs -mv /FlareData/output_8/WBS_PM_RATED_CDRS/tbl_dt=$1/*  /FlareData/output8_backup/WBS_PM_RATED_CDRS/tbl_dt=${1}/${currdate}${runTime}
retVal=$?
if [ $retVal -eq 0 ];
then
echo "moved data to backup"
else
echo "Didn't find data to move to backup"
fi
bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/INC_driver_localJar_20190417.sh /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/tmp/ $1  "0 1 2 3 4 5 6 7 8 9" $maxSeq $currMaxSeq
echo "First Return code $retVal"
exit
retVal=$?
if [ $retVal -eq 0 ];
then
#split command
echo $currMaxSeq>>${max_seq_file_name}
rmdir /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/incoming/$currDate
rmdir /mnt/beegfs_bsl/live/DB_extract_lz/WBS_PM_RATED_CDRS/incoming/$currDate
#mv /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/tmp/$currDate /mnt/beegfs_bsl/live/DB_extract_lz/WBS_PM_RATED_CDRS/incoming/
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
extractedRecordsNo=$(cat /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/" --general_message "$extractedRecordsNo extracted records"  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
#ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Move PM_RATED to incoming directory Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/"  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
else
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract PM_RATED Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/"  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
