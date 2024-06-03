#!/bin/bash
if [ "$#" -ne 1 ]
 then
echo "Wrong number of parameters "
exit 1
else
#today=$(/bin/date --date "$(date)" +%Y%m%d)
today=$(/bin/date --date "$1" +%Y%m%d)
fi
dayToProcess=$today
for i in {1..8}; 
do
	echo "$i  ==================>Processing: $dayToProcess"
	java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar  -in /mnt/beegfs/CDR/AIR_ADJ_DA/ -out /mnt/beegfs/CDR/finalreport/AIR_ADJ_DA/ -od -of -tf 2 -nt 30  -iffl $dayToProcess -dp 15 -op lineCount -mv /mnt/beegfs/structured/CDR/AIR_ADJ_DA/ -rg "(?<=_)($dayToProcess)(?=-)"
        dayToProcess=$(date +%Y%m%d -d "$today + $i day")

done
