#!/bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')
yyest=$(date -d '-2 day' '+%Y%m%d')
date=$(date -d '-1 day' '+%Y%m%d')
#Send Start Job Email
#echo -e "CronJob \"extract_CUG_ACCESS_FEES.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "daasuser@edge01001.mtn.com" -s "Nigeria Crontab Notification - Extract CUG_ACCESS_FEES Data Started..." "yulbeh@ligadata.com, moh-khalawi@ligadata.com, wsbayee@ligadata.com"

bash /home/daasuser/fahmi/CUG_ACCESS_FEES/driver_CUG_ACCESS_FEES.sh /home/daasuser/fahmi/CUG_ACCESS_FEES/ /home/daasuser/fahmi/CUG_ACCESS_FEES/tmp $yest $yyest $date

#Send Job Finished Email
extractedRecordsNo=$(cat /home/daasuser/fahmi/CUG_ACCESS_FEES/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#echo -e "CronJob \"exctract_CUG_ACCESS_FEES\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n" | mailx -r "daasuser@edge01001.mtn.com" -s "Nigeria Crontab Notification - Extract CUG_ACCESS_FEES Data Finished..." "yulbeh@ligadata.com, moh-khalawi@ligadata.com, wsbayee@ligadata.com, fabuirshaid@ligadata.com"
echo $extractedRecordsNo
