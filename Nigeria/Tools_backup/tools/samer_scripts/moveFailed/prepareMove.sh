#!/bin/bash
Usage(){ echo "Usage: [ --config  ] [--feed] [--date] [--hdfs]";}
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 22 --status 0 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName
currentDate=`date +%Y%m%d%H%M%S`
processingDt=$(echo "$currentDate"|cut -c1-8)
echo "processing: $(date -d $processingDt +%Y-%m-%d)"
fileLst= grep  "ERROR - Failed to move file" /mnt/beegfs/FlareLoadCluster/logs/logs/KamanjaManager/Engine_Node*.log | grep " to dir " | cut -d ' ' -f 12,15 | sort | uniq | sed -e "s/file://"|sed -e 's/$/\//' |sed -e "s/Failed to move file://" >"/mnt/beegfs/tools/samer_scripts/moveFailed/movedFiles/failedFileLst_$currentDate.txt"

while read LINE
do
echo "moving: $LINE"
mv  $LINE
#echo "mv  $LINE"
done</mnt/beegfs/tools/samer_scripts/moveFailed/movedFiles/failedFileLst_$currentDate.txt
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 22 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/samer_scripts/moveFailed/movedFiles/failedFileLst_$currentDate.txt" --brokers $kafkaHostList --topic $TopicName

