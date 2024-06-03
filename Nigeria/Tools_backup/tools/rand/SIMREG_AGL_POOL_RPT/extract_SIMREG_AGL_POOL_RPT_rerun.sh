#!/bin/bash
yest=$1
removeDir=$(date -d '-60 day' '+%Y%m%d')

#rm -r /mnt/beegfs/tools/rand/SIMREG_AGL_POOL_RPT/logs/${removeDir}
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: extract_SIMREG_AGL_POOL_RPT. Status: Started. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

#Send Start Job Email
# echo -e "CronJob \"extract_SIMREG_AGL_POOL_RPT.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<extract SIMREG_AGL_POOL_RPT Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com,  wsbayee@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, samer@ligadata.com, krishna@ligadata.com, a.sady@ligadata.com"

bash /mnt/beegfs/tools/rand/SIMREG_AGL_POOL_RPT/driver_SIMREG_AGL_POOL_RPT.sh /mnt/beegfs/tools/rand/SIMREG_AGL_POOL_RPT /mnt/beegfs/FlareData/CDR/SIMREG_AGL_POOL_RPT/tmp/ $yest 
retVal=$?
if [ $retVal -eq 0 ];
then
############### mv /mnt/beegfs/FlareData/CDR/SIMREG_AGL_POOL_RPT/tmp/$yest /mnt/beegfs/FlareData/CDR/SIMREG_AGL_POOL_RPT/incoming/
#Send Job Finished Email
# extractedRecordsNo=$(cat /mnt/beegfs/tools/rand/SIMREG_AGL_POOL_RPT/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#echo -e "CronJob \"extract_SIMREG_AGL_POOL_RPT.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<extract SIMREG_AGL_POOL_RPT Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, samer@ligadata.com, krishna@ligadata.com, a.sady@ligadata.com"
echo "Job: extract_SIMREG_AGL_POOL_RPT. Status: Finished. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
#echo -e "CronJob \"extract_SIMREG_AGL_POOL_RPT.sh\" Failed at $(date +"%T"), for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<extract SIMREG_AGL_POOL_RPT Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, samer@ligadata.com, krishna@ligadata.com, a.sady@ligadata.com"
echo "Job: extract_SIMREG_AGL_POOL_RPT. Status: Failed. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

fi
