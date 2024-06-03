#!/bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')
removeDir=$(date -d '-60 day' '+%Y%m%d')

#rm -r /mnt/beegfs/tools/rand/AGL_CRM_COUNTRY_MAP/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: extract_AGL_CRM_COUNTRY_MAP. Status: Started. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
# echo -e "CronJob \"extract_AGL_CRM_COUNTRY_MAP.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01002.mtn.com" -s "DAAS_Note_MTN_NG_<extract AGL_CRM_COUNTRY_MAP Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com,  wsbayee@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, samer@ligadata.com, krishna@ligadata.com, a.sady@ligadata.com"

bash /mnt/beegfs/tools/rand/AGL_CRM_COUNTRY_MAP/driver_AGL_CRM_COUNTRY_MAP.sh /mnt/beegfs/tools/rand/AGL_CRM_COUNTRY_MAP /mnt/beegfs/FlareData/CDR/AGL_CRM_COUNTRY_MAP/tmp/ $yest 
retVal=$?
if [ $retVal -eq 0 ];
then
############### mv /mnt/beegfs/FlareData/CDR/AGL_CRM_COUNTRY_MAP/tmp/$yest /mnt/beegfs/FlareData/CDR/AGL_CRM_COUNTRY_MAP/incoming/
#Send Job Finished Email
# extractedRecordsNo=$(cat /mnt/beegfs/tools/rand/AGL_CRM_COUNTRY_MAP/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#echo -e "CronJob \"extract_AGL_CRM_COUNTRY_MAP.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01002.mtn.com" -s "DAAS_Note_MTN_NG_<extract AGL_CRM_COUNTRY_MAP Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, samer@ligadata.com, krishna@ligadata.com, a.sady@ligadata.com"
echo "Job: extract_AGL_CRM_COUNTRY_MAP. Status: Finished. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
#echo -e "CronJob \"extract_AGL_CRM_COUNTRY_MAP.sh\" Failed at $(date +"%T"), for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<extract AGL_CRM_COUNTRY_MAP Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, samer@ligadata.com, krishna@ligadata.com, a.sady@ligadata.com"
echo "Job: exctract_AGL_CRM_COUNTRY_MAP. Status: Failed. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

fi
