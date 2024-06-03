#!/bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')

#Send Start Job Email
#echo -e "CronJob \"extract_CB_SERV_MAST_VIEW.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "daasuser@edge01001.mtn.com" -s "Nigeria Crontab Notification - Extract CB_SERV_MAST_VIEW Data Started..." "yulbeh@ligadata.com, moh-khalawi@ligadata.com, wsbayee@ligadata.com"

bash /home/daasuser/fahmi/CB_SERV_MAST_VIEW/driver_CB_SERV_MAST_VIEW.sh /home/daasuser/fahmi/CB_SERV_MAST_VIEW/ /home/daasuser/fahmi/CB_SERV_MAST_VIEW/tmp $yest "0 1 2 3 4 5 6 7 8 9"

#Send Job Finished Email
extractedRecordsNo=$(cat /opt/flare/DEV/Tools/ExtractOracleTable/CB_SERV_MAST_VIEW/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#echo -e "CronJob \"exctract_CB_SERV_MAST_VIEW\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n" | mailx -r "daasuser@edge01001.mtn.com" -s "Nigeria Crontab Notification - Extract SMILE_SUBSCRIBER_WALLETS Data Finished..." "yulbeh@ligadata.com, moh-khalawi@ligadata.com, wsbayee@ligadata.com, fabuirshaid@ligadata.com"
echo $extractedRecordsNo
