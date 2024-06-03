#!/bin/bash
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] [--hdfs]";}
currentDate=`date +%Y%m%d%H%M%S`
PATH="/home/daasuser/samer_scripts/GGSN_Rename/test"
#/mnt/beegfs/live/GGSN/delayed_incoming/
echo "$PATH"
file_lst=$(/bin/find  ${PATH} -type f)
#  |/bin/head -10 )
#echo $file_lst
for i in "${file_lst[@]}"
do
   echo "$i"
fileContents=$(/bin/head -1 ${i})
#echo "$fileContents"
transaction_date=$(echo $fileContents |/bin/cut -d '|' -f 15|/bin/cut -d ' ' -f 1)
#echo "$transaction_date"
echo "${i} :${transaction_date}" 
#echo "$fileContents"
   # or do whatever with individual element of the array
done

#fileLst= grep  "ERROR - Failed to move file" /mnt/beegfs/FlareLoadCluster/logs/logs/KamanjaManager/Engine_Node*.log | grep " to dir " | cut -d ' ' -f 12,15 | sort | uniq | sed -e "s/file://"|sed -e 's/$/\//' |sed -e "s/Failed to move file://" >"./failedFileLst_$currentDate.txt"
#while read LINE
#do

