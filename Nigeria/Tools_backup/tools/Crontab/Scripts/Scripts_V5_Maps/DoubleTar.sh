#!/bin/bash

date=$1

####report_folder=/mnt/beegfs/tmp/archive_reports/doubletarring
report_folder=/mnt/beegfs/tools/Crontab/logs/doubletarring/${date}
from=/mnt/beegfs/production/archived
hdfs=/nas/share02/dfs/production/doubleTarred
removeDate=`date '+%C%y%m%d' -d "$end_date-65 days"`

mkdir ${report_folder}
rm -r /mnt/beegfs/tools/Crontab/logs/doubletarring/${removeDate}
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
# delete files
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $hdfs/MAPS_2G_CELL_D/$date $hdfs/MAPS_2G_CELL_W_BH/$date $hdfs/MAPS_3G_CELL_D/$date $hdfs/MAPS_3G_CELL_D_BH/$date $hdfs/MAPS_3G_CELL_W/$date $hdfs/MAPS_3G_CELL_M/$date $hdfs/MAPS_3G_CELL_M_BH/$date $hdfs/MAPS_3G_CN_D/$date $hdfs/MAPS_3G_CN_D_BH/$date $hdfs/MAPS_3G_CN_W_BH/$date $hdfs/MAPS_3G_CN_M/$date $hdfs/MAPS_3G_CN_W/$date $hdfs/MAPS_2G_CN_M_DATA_BH/$date $hdfs/MAPS_2G_CN_W_BH/$date -nt 10 -vad -rmp $from/#fn/$date/$date -out $report_folder -dp 15 -tf 1 -nosim -lbl double_taring_delete_ -of -od -fnfp 6

  bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 6 --status 1 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar V5_Maps files already double tarred"
### push to kafka here

  bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 6 --status 0 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "double tar files V5_Maps"
### push to kafka here
# tarring most of the feeds
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $infolder/MAPS_2G_CELL_D/$date $infolder/MAPS_2G_CELL_W_BH/$date $infolder/MAPS_3G_CELL_D/$date $infolder/MAPS_3G_CELL_D_BH/$date $infolder/MAPS_3G_CELL_W/$date $infolder/MAPS_3G_CELL_M/$date $infolder/MAPS_3G_CELL_M_BH/$date $infolder/MAPS_3G_CN_D/$date $infolder/MAPS_3G_CN_D_BH/$date $infolder/MAPS_3G_CN_W_BH/$date $infolder/MAPS_3G_CN_M/$date $infolder/MAPS_3G_CN_W/$date $infolder/MAPS_2G_CN_M_DATA_BH/$date $infolder/MAPS_2G_CN_W_BH/$date -outarchive               "$hdfs/#1/#2" -nt 10 -cvzf -tarPrefix "#1-#2" -rgrm "$from/#1/#2/#2" -groupregex "(MAPS_2G_CELL_D|MAPS_2G_CELL_W_BH|MAPS_3G_CELL_D|MAPS_3G_CELL_D_BH|MAPS_3G_CELL_W|MAPS_3G_CELL_M|MAPS_3G_CELL_M_BH|MAPS_3G_CN_D|MAPS_3G_CN_D_BH|MAPS_3G_CN_W_BH|MAPS_3G_CN_M|MAPS_3G_CN_W|MAPS_2G_CN_M_DATA_BH|MAPS_2G_CN_W_BH)-*.([0-9]{8})" -out $report_folder -dp 15 -tf 1 -ts 120 -iffl "(MAPS_2G_CELL_D|MAPS_2G_CELL_W_BH|MAPS_3G_CELL_D|MAPS_3G_CELL_D_BH|MAPS_3G_CELL_W|MAPS_3G_CELL_M|MAPS_3G_CELL_M_BH|MAPS_3G_CN_D|MAPS_3G_CN_D_BH|MAPS_3G_CN_W_BH|MAPS_3G_CN_M|MAPS_3G_CN_W|MAPS_2G_CN_M_DATA_BH|MAPS_2G_CN_W_BH)-*.([0-9]{8})" -nosim -lbl double_taring_  2>&1 | tee "$report_folder/double_taring_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/double_taring_$mytime.log)
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 6 --status 1 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "double tar files V5_Maps: $numberOfFiles"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 6 --status 0 --hostname "$hostName" --step 3 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar files V5_Maps"
### push to kafka here

# delete files
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $hdfs/MAPS_2G_CELL_D/$date $hdfs/MAPS_2G_CELL_W_BH/$date $hdfs/MAPS_3G_CELL_D/$date $hdfs/MAPS_3G_CELL_D_BH/$date $hdfs/MAPS_3G_CELL_W/$date $hdfs/MAPS_3G_CELL_M/$date $hdfs/MAPS_3G_CELL_M_BH/$date $hdfs/MAPS_3G_CN_D/$date $hdfs/MAPS_3G_CN_D_BH/$date $hdfs/MAPS_3G_CN_W_BH/$date $hdfs/MAPS_3G_CN_M/$date $hdfs/MAPS_3G_CN_W/$date $hdfs/MAPS_2G_CN_M_DATA_BH/$date $hdfs/MAPS_2G_CN_W_BH/$date -nt 10 -vad -rmp $from/#fn/$date/$date -out $report_folder -dp 15 -tf 1 -nosim -lbl double_taring_delete_ -of -od -fnfp 6

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 6 --status 1 --hostname "$hostName" --step 3 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete mini tar files V5_Maps"
### push to kafka here
