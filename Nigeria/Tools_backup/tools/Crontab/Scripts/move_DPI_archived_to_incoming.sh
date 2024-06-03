#!/bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')
incoming_dir=/mnt/beegfs/live/DPI_CDR/incoming/
archived_dir=/mnt/beegfs/production/archived/DPI
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)

echo -e "CronJob \"move dpi archived to incoming\" Started at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Move_DPI_Arachived_To_Incoming Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "mv $archived_dir/$yest $incoming_dir"
mv $archived_dir/$yest $incoming_dir

echo -e "CronJob \"move dpi archived to incoming\" Finished at $(date +"%T") on edge01001, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Move_DPI_Arachived_To_Incoming Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"

