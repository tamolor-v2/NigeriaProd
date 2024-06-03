#!/bin/bash
###yest=$(date -d '-1 day' '+%Y%m%d')
yest=201712
#Send Start Job Email
###echo -e "CronJob \"extract_CALL_REASON_MONTHLY.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "daasuser@edge01001.mtn.com" -s "Nigeria Crontab Notification - Extract CALL_REASON_MONTHLY Data Started..." "yulbeh@ligadata.com, moh-khalawi@ligadata.com, wsbayee@ligadata.com"

bash /data/data01/spool/working_dir/CALL_REASON_MONTHLY/driver_CALL_REASON_MONTHLY.sh /data/data01/spool/working_dir/CALL_REASON_MONTHLY /mnt/beegfs/live_bib/spool/CALL_REASON_MONTHLY/ $yest "5"

#Send Job Finished Email
###extractedRecordsNo=$(cat /data/data01/spool/working_dir/CALL_REASON_MONTHLY/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
###echo -e "CronJob \"exctract_CALL_REASON_MONTHLY.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n" | mailx -r "daasuser@edge01001.mtn.com" -s "Nigeria Crontab Notification - Extract CALL_REASON_MONTHLY Data Finished..." "yulbeh@ligadata.com, moh-khalawi@ligadata.com, wsbayee@ligadata.com"

