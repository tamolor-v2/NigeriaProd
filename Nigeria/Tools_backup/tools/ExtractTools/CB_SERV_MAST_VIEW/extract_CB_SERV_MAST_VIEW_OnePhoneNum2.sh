#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$(date -d '-1 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
removeDir=$(date -d '-60 day' '+%Y%m%d')
rm -r /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs_bsl/tools/Crontab/logs/general_logs
#Send Start Job Email
#rm -rf  /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp/$yest
ssh edge01002 " echo -e 'CronJob \"extract_CB_SERV_MAST_VIEW.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CB_SERV_MAST_VIEW started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: CB_SERV_MAST_VIEW. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/ /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp $yest " 2 "
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email

 mv /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/CB_SERV_MAST_VIEW/incoming/
extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"exctract_CB_SERV_MAST_VIEW\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_< extract CB_SERV_MAST_VIEW Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: CB_SERV_MAST_VIEW. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
ssh edge01002 " echo -e 'CronJob \"exctract_CB_SERV_MAST_VIEW\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract CB_SERV_MAST_VIEW Fialed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: CB_SERV_MAST_VIEW. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
