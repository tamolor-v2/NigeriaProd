#!/bin/bash
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/SupportTeam/logs
ListofFiles=$(ls /mnt/beegfs/tools/SupportTeam/data/*.csv)




echo "Starting copy process to flytxt server" >> ${generalLogs}/logs_${currDate}.log
ssh edge01002 " echo -e 'CronJob Flytxt Refill Files Started at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Flytxt Refill Files Started at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

scp /mnt/beegfs/tools/SupportTeam/data/*.csv -i /mnt/beegfs/tools/SupportTeam/key.pem daas_user@10.1.204.41:/ftp-live/realtime/refill_im/

echo "Successfully copy files to flytxt server: '$ListofFiles' " >> ${generalLogs}/logs_${currDate}.log

mv /mnt/beegfs/tools/SupportTeam/data/*.csv /mnt/beegfs/tools/SupportTeam/data/archive

echo "Successfully move files to archive: '$ListofFiles' " >> ${generalLogs}/logs_${currDate}.log

ssh edge01002 " echo -e 'CronJob Flytxt Refill Files Finished at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Flytxt Refill Files Finished at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: Flytxt Refill Files Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/logs_${currDate}.log

chmod 777  ${generalLogs}/logs_${currDate}.log
