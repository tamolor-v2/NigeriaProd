emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email1.dat)
echo -e "CronJob \"line_count_archived_withValidation.sh\" Start Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
