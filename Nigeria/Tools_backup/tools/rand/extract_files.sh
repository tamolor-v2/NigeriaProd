#!/bin/bash
#
#
#startDate=$(date -d "$1" +%Y%m%d)
#endDate=$(date -d "$2" +%Y%m%d)
#dt=$(date -d "$startDate" +%Y%m%d)
dt=$1
from=/mnt/beegfs/production/data/flare_production/feeds
copyTo=/mnt/beegfs/production/archived/
report=/mnt/beegfs/tools/Crontab/logs/archive_reports
fileOps_path="/mnt/beegfs/tools/fileOps"
#while [ $dt -le $endDate ]; do
echo $dt

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=$fileOps_path/log4j2.xml -jar $fileOps_path/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in  $from/SAG_API/processed/$dt   -groupregex "(SAG_API)" -out $report/$dt -xvzf $copyTo/\#1  -od -of -tf 1 -nt 16 -dp 15 -nosim

#dt=$(date -I -d "$dt + 1 day")
#dt=$(date -d "$dt" +%Y%m%d)

#done
