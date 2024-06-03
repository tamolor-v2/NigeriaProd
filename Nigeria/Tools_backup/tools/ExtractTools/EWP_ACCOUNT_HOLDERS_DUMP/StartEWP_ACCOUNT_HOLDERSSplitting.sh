#/bin/bash
DATE_TIME=`date '+%Y%m%d%H%M%S'`
LOG_LOCATION=/mnt/beegfs_bsl/Deployment/DEV/logs/EWP_ACCOUNT_HOLDERS
SCRIPTS_LOCAION=/mnt/beegfs_bsl/Deployment/DEV/scripts/splittingScirpts
FILE_NAME=${DATE_TIME}.log
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
#Send start email
ssh edge01002 "echo -e 'CronJob \"StartEWP_ACCOUNT_HOLDERSSplitting.sh\" Start Account_Holder Splitting at $(date +"%T") on edge01002, for day: $DATE_TIME \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<Account_Holder Splitting Started at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"

echo "job started at : ${DATE_TIME}"
${SCRIPTS_LOCAION}/Split_EWP_ACCOUNT_HOLDERS_DUMP.sh  --landing_dir /mnt/beegfs_bsl/live/EWP_ACCOUNT_HOLDERS_DUMP/incoming/ --working_dir /mnt/beegfs_bsl/live/EWP_ACCOUNT_HOLDERS_DUMP/work/ --kamanja_incoming_dir /mnt/beegfs_bsl/live/EWP_ACCOUNT_HOLDERS_DUMP/split/ >> ${LOG_LOCATION}/${FILE_NAME}
retVal=$?
if [ $retVal -eq 0 ];
then
#Send finish email
ssh edge01002 "echo -e 'CronJob \"StartEWP_ACCOUNT_HOLDERSSplitting.sh\" Finished Account_Holder Splitting at $(date +"%T") on edge01002, for day: $DATE_TIME \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<Account_Holder Splitting Finished at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
else
ssh edge01002 "echo -e 'CronJob \"StartEWP_ACCOUNT_HOLDERSSplitting.sh\" Failed at $(date +"%T") on edge01002, for day: $DATE_TIME \n' | mailx -r 'DAAS_Alert_NG@edge01002.mtn.com -s 'DAAS_Alert_MTN_NG_<Account_Holder Splitting Failed at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
fi

