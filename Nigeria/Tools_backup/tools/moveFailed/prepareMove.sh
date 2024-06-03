#!/bin/bash
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] [--hdfs]";}
currentDate=`date +%Y%m%d%H%M%S`
#bash  /mnt/beegfs/tools/moveFailed/CopyToLocal.scala
echo "Finished copying log files"
fileLst= grep  "ERROR - Failed to move file" /mnt/beegfs/tools/moveFailed/KamanjaLogs/Engine_Node*.log | grep " to dir " | cut -d ' ' -f 12,15 | sort | uniq | sed -e "s/file://"|sed -e 's/$/\//' |sed -e "s/Failed to move file://" >"./failedFileLst_$currentDate.txt"

while read LINE
do
echo "moving: $LINE"
mv  $LINE
echo "mv  $LINE"
done<./failedFileLst_$currentDate.txt

