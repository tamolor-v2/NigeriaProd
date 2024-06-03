#!/bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')

mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
removeDir=$(date -d '-60 day' '+%Y%m%d')
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs

#DPI_CDR
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/DPI_CDR/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/DPI_CDR -re -rg  "^DPI-([0-9]{8})-([0-9]{2})-.*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_DPI_CDR_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_DPI_CDR_$mytime.log"


