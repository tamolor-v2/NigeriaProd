#!/bin/bash
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] [--hdfs]";}
currentDate=`date +%Y%m%d%H%M%S`
dt=$(date -d "$1" +"%Y-%m-%d")
echo "processing: $dt"
fileLst= zgrep  "ERROR - Failed to move file" /mnt/beegfs/FlareLoadCluster/logs/logs/Archive/$dt/KamanjaManager/*.gz | grep " to dir " | cut -d ' ' -f 12,15 | sort | uniq | sed -e "s/file://"|sed -e 's/$/\//' |sed -e "s/Failed to move file://"  >"/mnt/beegfs/tools/samer_scripts/moveFailed/movedFiles/failedFileLst_$currentDate.txt"

while read LINE
do
echo "moving: $LINE"
#mv  $LINE
#echo "mv  $LINE"
done</mnt/beegfs/tools/samer_scripts/moveFailed/movedFiles/failedFileLst_$currentDate.txt
