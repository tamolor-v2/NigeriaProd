#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$1
week7=$(date  -d "$yest -6 day" "+%Y%m%d")
week6=$(date  -d "$yest -5 day" "+%Y%m%d")
week5=$(date  -d "$yest -4 day" "+%Y%m%d")
week4=$(date  -d "$yest -3 day" "+%Y%m%d")
week3=$(date  -d "$yest -2 day" "+%Y%m%d")
week2=$(date  -d "$yest -1 day" "+%Y%m%d")
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
removeDir=$(date -d "$yest -60 day" "+%Y%m%d")
rm -r /mnt/beegfs_bsl/tools/ExtractTools/MNP_PORTING_BROADCAST/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs_bsl/tools/Crontab/logs/general_logs
echo "Job: extract_MNP_PORTING_BROADCAST. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
echo -e "CronJob \"extract_MNP_PORTING_BROADCAST.sh\" Started at $(date +"%T") on edge01001, from day: $week7 until $yest\n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<extract MNP_PORTING_BROADCAST Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
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
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week7
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week6
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week5
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week4
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week3
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$week2
  hdfs dfs -rmr /FlareData/output_8/MNP_PORTING_BROADCAST/tbl_dt=$yest
  mv /mnt/beegfs_bsl/FlareData/CDR/MNP_PORTING_BROADCAST/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/MNP_PORTING_BROADCAST/incoming/
fi
echo -e "CronJob \"exctract_MNP_PORTING_BROADCAST.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, from day: $week7 until $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<extract MNP_PORTING_BROADCAST Finished from $week7 to  $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "Job: extract_MNP_PORTING_BROADCAST. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
echo -e "CronJob \"exctract_MNP_PORTING_BROADCAST.sh\" Failed at $(date +"%T"), from day: $week7 until $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<extract MNP_PORTING_BROADCAST Failed from $week7 to  $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "Job: extract_MNP_PORTING_BROADCAST. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
