#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$1
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /nas/share05/tools/Crontab/Scripts/email.dat)
rm -r /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/${removeDir}

currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/nas/share05/tools/Crontab/logs/general_logs
echo "Job: exctract_PM_RATED. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"

bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/driver.sh /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/tmp/ $yest "00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23" "0 1 2 3 4 5 6 7 8 9"
retVal=$?
if [ $retVal -eq 0 ];
then
rm -r /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/incoming/$yest
mv /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/tmp/$yest /mnt/beegfs_bsl/FlareData/CDR/WBS_PM_RATED_CDRS/incoming/
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
extractedRecordsNo=$(cat /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Move PM_RATED to incoming directory Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
else
ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract PM_RATED Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
