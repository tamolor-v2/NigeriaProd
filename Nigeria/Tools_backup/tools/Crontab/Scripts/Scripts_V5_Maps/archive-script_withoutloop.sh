#!/bin/bash

date=$1

####report_folder=/mnt/beegfs/tmp/archive_reports/
report_folder=/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${date}
infolder=/mnt/beegfs/production/archived
removeDir=`date '+%C%y%m%d' -d "$end_date-63 days"`
mkdir ${report_folder}
rm -r /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${removeDir}
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 0 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original message already mini tarred V5_Maps"
### push to kafka here
# delete files
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in $infolder/MAPS_2G_CELL_D/$date/$date $infolder/MAPS_2G_CELL_W_BH/$date/$date $infolder/MAPS_3G_CELL_D/$date/$date $infolder/MAPS_3G_CELL_D_BH/$date/$date $infolder/MAPS_3G_CELL_W/$date/$date $infolder/MAPS_3G_CELL_M/$date/$date $infolder/MAPS_3G_CELL_M_BH/$date/$date $infolder/MAPS_3G_CN_D/$date/$date $infolder/MAPS_3G_CN_D_BH/$date/$date $infolder/MAPS_3G_CN_W_BH/$date/$date $infolder/MAPS_3G_CN_M/$date/$date $infolder/MAPS_3G_CN_W/$date/$date $infolder/MAPS_2G_CN_M_DATA_BH/$date/$date $infolder/MAPS_2G_CN_W_BH/$date/$date -nt 32 -vad -rmp $infolder -out $report_folder -dp 15 -tf 1 -nosim -lbl mini_delete_V5_Maps -of -od -fnfp 5

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 1 --hostname "$hostName" --step 1 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original message already mini tarred V5_Maps"
### push to kafka here

 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 0 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar most of feeds V5_Maps"
### push to kafka here

# tarring most of the feeds
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  -outarchive "$infolder/#3/#1/#1" -nt 32 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^([0-9]{8})([0-9]{2}).*().*$" -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*().*$" -nosim -lbl mini_taring_V5_Maps  2>&1 | tee "$report_folder/mini_taring_V5_Maps_$mytime.log"

numberOfFiles=$(tail -n 3 $report_folder/mini_taring_V5_Maps_$mytime.log)
 bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 1 --hostname "$hostName" --step 2 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "mini tar most of feeds : $numberOfFiles"
### push to kafka here

# delete files
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=./log4j2.xml ./FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in $infolder/MAPS_2G_CELL_D/$date/$date $infolder/MAPS_2G_CELL_W_BH/$date/$date $infolder/MAPS_3G_CELL_D/$date/$date $infolder/MAPS_3G_CELL_D_BH/$date/$date $infolder/MAPS_3G_CELL_W/$date/$date $infolder/MAPS_3G_CELL_M/$date/$date $infolder/MAPS_3G_CELL_M_BH/$date/$date $infolder/MAPS_3G_CN_D/$date/$date $infolder/MAPS_3G_CN_D_BH/$date/$date $infolder/MAPS_3G_CN_W_BH/$date/$date $infolder/MAPS_3G_CN_M/$date/$date $infolder/MAPS_3G_CN_W/$date/$date $infolder/MAPS_2G_CN_M_DATA_BH/$date/$date $infolder/MAPS_2G_CN_W_BH/$date/$date -nt 32 -vad -rmp $infolder -out $report_folder -dp 15 -tf 1 -nosim -lbl mini_delete_V5_Maps_{FeedName{ -of -od -fnfp 5

bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 1 --hostname "$hostName" --step 39 --log_directory "$report_folder" --brokers $kafkaHostList --topic $TopicName  --general_message "delete original files"


#MAPS_2G_CELL_D
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_2G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CELL_D\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_2G_CELL_D/$date  -outarchive "$infolder/MAPS_2G_CELL_D/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_2GCell_Daily_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_2GCell_Daily_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_2G_CELL_D  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_2G_CELL_D_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_2G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CELL_D\""

#MAPS_2G_CELL_W_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_2G_CELL_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CELL_W_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_2G_CELL_W_BH/$date  -outarchive "$infolder/MAPS_2G_CELL_W_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_2GCell_Weekly_BH_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_2GCell_Weekly_BH_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_2G_CELL_W_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_2G_CELL_W_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_2G_CELL_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CELL_W_BH\""

#MAPS_3G_CELL_D
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 5 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_D\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CELL_D/$date  -outarchive "$infolder/MAPS_3G_CELL_D/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3GCell_Daily_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3GCell_Daily_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CELL_D  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CELL_D_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 5 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_D\""

#MAPS_3G_CELL_D_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 6 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CELL_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_D_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CELL_D_BH/$date  -outarchive "$infolder/MAPS_3G_CELL_D_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3GCell_Daily_BH_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3GCell_Daily_BH_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CELL_D_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CELL_D_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 6 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CELL_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_D_BH\""

#MAPS_3G_CELL_W
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 7 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CELL_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_W\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CELL_W/$date  -outarchive "$infolder/MAPS_3G_CELL_W/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3GCell_Weekly_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3GCell_Weekly_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CELL_W  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CELL_W_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 7 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CELL_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_W\""

#MAPS_3G_CELL_M
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 8 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CELL_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_M\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CELL_M/$date  -outarchive "$infolder/MAPS_3G_CELL_M/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3GCell_Monthly_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3GCell_Monthly_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CELL_M  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CELL_M_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 8 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CELL_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_M\""

#MAPS_3G_CELL_M_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 9 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CELL_M_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_M_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CELL_M_BH/$date  -outarchive "$infolder/MAPS_3G_CELL_M_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3GCell_Monthly_BH_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3GCell_Monthly_BH_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CELL_M_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CELL_M_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 9 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CELL_M_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CELL_M_BH\""

#MAPS_3G_CN_D
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 10 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CN_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_D\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CN_D/$date  -outarchive "$infolder/MAPS_3G_CN_D/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3G_Country_Daily_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3G_Country_Daily_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CN_D  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CN_D_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 10 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CN_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_D\""

#MAPS_3G_CN_D_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 11 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CN_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_D_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CN_D_BH/$date  -outarchive "$infolder/MAPS_3G_CN_D_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3G_Country_Daily_BH_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3G_Country_Daily_BH_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CN_D_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CN_D_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 11 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CN_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_D_BH\""

#MAPS_3G_CN_W_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 12 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_W_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CN_W_BH/$date  -outarchive "$infolder/MAPS_3G_CN_W_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3G_Country_Weekly_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3G_Country_Weekly_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CN_W_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CN_W_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 12 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_W_BH\""

#MAPS_3G_CN_M
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 13 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CN_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_M\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CN_M/$date  -outarchive "$infolder/MAPS_3G_CN_M/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3G_Country_Monthly_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3G_Country_Monthly_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CN_M  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CN_M_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 13 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CN_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_M\""

#MAPS_3G_CN_W
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 14 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_3G_CN_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_W\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_3G_CN_W/$date  -outarchive "$infolder/MAPS_3G_CN_W/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_3G_Country_Weekly_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_3G_Country_Weekly_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_3G_CN_W  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_3G_CN_W_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 14 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_3G_CN_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_3G_CN_W\""

#MAPS_2G_CN_M_DATA_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 15 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_2G_CN_M_DATA_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CN_M_DATA_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_2G_CN_M_DATA_BH/$date  -outarchive "$infolder/MAPS_2G_CN_M_DATA_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_2G_Country_Monthly_Data_BH_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_2G_Country_Monthly_Data_BH_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_2G_CN_M_DATA_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_2G_CN_M_DATA_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 15 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_2G_CN_M_DATA_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CN_M_DATA_BH\""

#MAPS_2G_CN_W_BH
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 0 --hostname \"$hostName\" --step 16 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini tar_{V}_MAPS_2G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CN_W_BH\""
time java -Xmx20g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/MAPS_2G_CN_W_BH/$date  -outarchive "$infolder/MAPS_2G_CN_W_BH/#1/#1" -nt 16 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex "^Nigeria_BIB_2G_Country_Weekly_BH_([0-9]{8})([0-9]{2}).*zip$"  -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^Nigeria_BIB_2G_Country_Weekly_BH_([0-9]{8})([0-9]{2}).*zip$" -nosim -lbl mini_taring_{V}_MAPS_2G_CN_W_BH  2>&1 | tee "$report_folder/mini_taring_{V}_MAPS_2G_CN_W_BH_$mytime.log"
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 5--status 1 --hostname \"$hostName\" --step 16 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/archive_reports/${currDate}/mini_tar_{V}_MAPS_2G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"MiniTar MAPS_2G_CN_W_BH\""
