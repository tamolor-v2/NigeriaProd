#!/bin/bash
if [ "$#" -ne 4 ]
 then
echo "Wrong number of parameters "
exit 1
else
#today=$(/bin/date --date "$(date)" +%Y%m%d)
today=$(/bin/date --date "$1" +%Y%m%d)
fi
dayToProcess=$today
regex="(?<=_)($dayToProcess)(?=-)"
for i in {1..30};
do
        echo "$i  ==================>Processing: $dayToProcess"
        java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar ./FileOps_2.11-0.1.jar  -in $2  -out $3 -od -of -tf 2 -nt 30  -iffl $dayToProcess -dp 15 -op lineCount -mv $4 -rg "$dayToProcess"
        dayToProcess=$(date +%Y%m%d -d "$today + $i day")

done

