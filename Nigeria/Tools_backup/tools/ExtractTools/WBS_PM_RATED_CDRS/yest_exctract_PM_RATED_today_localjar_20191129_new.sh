#!/bin/bash
export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
yest=$(date -d '-1 day' '+%Y%m%d')
cd /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /nas/share05/tools/Crontab/Scripts/email.dat)
rm -r /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/${removeDir}
mkdir -p /nas/share05/tools/Crontab/logs/general_logs/${yest}
runTime=$(date +"%H%M%S")
currDate=$(date +"%Y%m%d")
echo "Running for today"
echo "running for $currDate"
echo "$currDate $runTime"

generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/nas/share05/tools/Crontab/logs/general_logs
find /mnt/beegfs_bsl/live/DB_extract_lz/WBS_PM_RATED_CDRS/incoming/ -type f -name '.*' -execdir sh -c 'mv -i "$0" "./${0#./.}"' {} \;
echo "Job: exctract_PM_RATED. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"

bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/yest_driver_localJar_20191129_new.sh /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS /nas/share05/FlareProd/Working/WBS_PM_RATED_CDRS/ $yest

echo "First Return code $retVal"
find /mnt/beegfs_bsl/live/DB_extract_lz/WBS_PM_RATED_CDRS/incoming/ -type f -name '.*' -execdir sh -c 'mv -i "$0" "./${0#./.}"' {} \;
sleep 65
retVal=$?
if [ $retVal -eq 0 ];
then
echo "hi1"
rmdir /nas/share05/FlareProd/Data/live/WBS_PM_RATED_CDRS/incoming/$yest
rmdir /mnt/beegfs_bsl/live/DB_extract_lz/WBS_PM_RATED_CDRS/incoming/$currDate
mv /nas/share05/FlareProd/Working/WBS_PM_RATED_CDRS/$currDate /nas/share05/FlareProd/Data/live/WBS_PM_RATED_CDRS/incoming/
retVal=$?
echo "main-Return Value: $retVal"
if [ $retVal -eq 0 ];
then
hour=$(date +"%H")
extractedRecordsNo=$(cat /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/$yest/$hour/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "hi2"
else
ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Alert_MTN_NG_<Move PM_RATED to incoming directory Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "hi3"
fi
else
ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Alert_MTN_NG_<extract PM_RATED Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
echo "hi4"
fi
