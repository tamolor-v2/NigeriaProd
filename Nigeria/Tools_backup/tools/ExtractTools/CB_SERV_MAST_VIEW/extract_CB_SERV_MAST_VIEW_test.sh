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
ssh edge01002 " echo -e 'CronJob \"extract_CB_SERV_MAST_VIEW.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract CB_SERV_MAST_VIEW started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: CB_SERV_MAST_VIEW. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/ /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp $yest "00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19" 
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/ /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp $yest "20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39"
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/ /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp $yest "40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59"
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/ /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp $yest "60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79"
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/ /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp $yest "80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99"
retVal=$?
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
####mv /mnt/beegfs_bsl/FlareData/CDR/CB_SERV_MAST_VIEW/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/CB_SERV_MAST_VIEW/incoming/
extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"exctract_CB_SERV_MAST_VIEW\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_< extract CB_SERV_MAST_VIEW Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: CB_SERV_MAST_VIEW. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
ssh edge01002 " echo -e 'CronJob \"exctract_CB_SERV_MAST_VIEW\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract CB_SERV_MAST_VIEW Fialed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: CB_SERV_MAST_VIEW. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
