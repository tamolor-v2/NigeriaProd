#!/bin/bash
yest=$1
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
rm -r /mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: exctract_CALL_REASON. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
echo -e "CronJob \"extract_CALL_REASON.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<extract CALL_REASON Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"

bash /mnt/beegfs/tools/ExtractTools/CALL_REASON_MISSING/driver_CALL_REASON.sh /mnt/beegfs/tools/ExtractTools/CALL_REASON_MISSING /mnt/beegfs/FlareData/CDR/CALL_REASON/tmp/ $yest "0 1 2 3 4 5 6 7 8 9"
retVal=$?
if [ $retVal -eq 0 ];
then
mv /mnt/beegfs/FlareData/CDR/CALL_REASON/tmp/$yest /mnt/beegfs/FlareData/CDR/CALL_REASON/incoming/
#Send Job Finished Email
extractedRecordsNo=$(cat /mnt/beegfs/tools/ExtractTools/CALL_REASON/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
echo -e "CronJob \"exctract_CALL_REASON.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<extract CALL_REASON Finished with $extractedRecordsNo extracted records for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "Job: exctract_CALL_REASON. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
echo -e "CronJob \"exctract_CALL_REASON.sh\" Failed at $(date +"%T"), for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<extract CALL_REASON Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "Job: exctract_CALL_REASON. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
