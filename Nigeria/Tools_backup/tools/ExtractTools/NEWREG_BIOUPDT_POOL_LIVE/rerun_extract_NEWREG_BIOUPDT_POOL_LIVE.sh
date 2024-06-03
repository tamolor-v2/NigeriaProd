#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$(date -d '-1 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
currDate=$1
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
echo "processing : $currDate"
#Send Start Job Email
#ssh edge01002 "  echo -e 'CronJob \"extract_NEWREG_BIOUPDT_POOL_LIVE.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract NEWREG_BIOUPDT_POOL_LIVE Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
bash /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/NEWREG_BIOUPDT_POOL_LIVE.sh ${currDate}
retVal=$?
echo "ret2=$retVal"
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
hadoop fs -mkdir -p /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${currDate}
echo "running: hadoop fs -rm /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${currDate}/*"
hadoop fs -rm /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${currDate}/*
hadoop fs -put /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/spool/NEWREG_BIOUPDT_POOL_LIVE_${currDate}.csv.gz /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${currDate}/
hive -e "msck repair table flare_8.NEWREG_BIOUPDT_POOL_LIVE"
echo "running: hadoop fs -put /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/spool/NEWREG_BIOUPDT_POOL_LIVE_${currDate} /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${currDate}/"
#echo "mv /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/spool/NEWREG_BIOUPDT_POOL_LIVE_${currDate}.csv.gz /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/old/"
#mv /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/spool/NEWREG_BIOUPDT_POOL_LIVE_${currDate}.csv.gz /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/old/
# extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/logs/$currDate/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"extract_AGL_CRM_LGA_MAP.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $currDate \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract NEWREG_BIOUPDT_POOL_LIVE Finished for $currDate with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")' '$emailReceiver' "
#echo "Job: extract_NEWREG_BIOUPDT_POOL_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
#ssh edge01002 " echo -e 'CronJob \"extract_NEWREG_BIOUPDT_POOL_LIVE.sh\" Failed at $(date +"%T"), for day: $currDate \n' | mailx -r 'DAAS_Alert_NG@edge01002.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract NEWREG_BIOUPDT_POOL_LIVE Failed for $currDate at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: extract_NEWREG_BIOUPDT_POOL_LIVE. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
fi
echo "retval=$retVal"
