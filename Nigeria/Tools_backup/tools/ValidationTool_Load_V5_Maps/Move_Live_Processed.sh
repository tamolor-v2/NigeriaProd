PIDFILE=/home/daasuser/PIDFiles/move_live_processed_to_archived_V5_Maps.pid
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)

check_pid_file(){
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    #exit 1
return 2
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
return 2
      #exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    #exit 1
return 2
  fi
fi
}

main_process(){
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
removeDir=$(date -d '-60 day' '+%Y%m%d')
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/general_logs
#move All_Processed_feeds
mkdir -p /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}
rm -r /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${removeDir}
mkdir -p  ${generalLogs}/${currDate}
echo "Job: move_live_processed_to_archivedV5_Maps. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#Move standard feeds
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}\" --brokers $kafkaHostList --topic $TopicName --general_message \"move Most feeds V5_Maps\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/ -out {MoveLogsFolder}/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_().*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_all_V5_Maps_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_All_V5_Maps_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_All_V5_Maps_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2  --status 1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_All_V5_Maps_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move most of feeds V5_Maps: $numberOfFiles\""



#MAPS_2G_CELL_D
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 2 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_2G_CELL_D\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_2G_CELL_D_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 2 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_2G_CELL_D/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_2G_CELL_D/ -re -rg "^Nigeria_BIB_2GCell_Daily_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_2G_CELL_D 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CELL_D_$mytime.log"


#MAPS_2G_CELL_W_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CELL_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_2G_CELL_W_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_2G_CELL_W_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CELL_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_2G_CELL_W_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_2G_CELL_W_BH/ -re -rg "^Nigeria_BIB_2GCell_Weekly_BH_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_2G_CELL_W_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CELL_W_BH_$mytime.log"


#MAPS_3G_CELL_D
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CELL_D\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CELL_D_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CELL_D/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CELL_D/ -re -rg "^Nigeria_BIB_3GCell_Daily_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CELL_D 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_D_$mytime.log"


#MAPS_3G_CELL_D_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 5 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CELL_D_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CELL_D_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 5 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CELL_D_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CELL_D_BH/ -re -rg "^Nigeria_BIB_3GCell_Daily_BH_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CELL_D_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_D_BH_$mytime.log"


#MAPS_3G_CELL_W
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 6 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CELL_W\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CELL_W_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 6 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CELL_W/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CELL_W/ -re -rg "^Nigeria_BIB_3GCell_Weekly_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CELL_W 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_W_$mytime.log"


#MAPS_3G_CELL_M
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 7 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CELL_M\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CELL_M_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 7 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CELL_M/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CELL_M/ -re -rg "^Nigeria_BIB_3GCell_Monthly_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CELL_M 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_M_$mytime.log"


#MAPS_3G_CELL_M_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 8 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_M_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CELL_M_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CELL_M_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 8 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_M_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CELL_M_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CELL_M_BH/ -re -rg "^Nigeria_BIB_3GCell_Monthly_BH_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CELL_M_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CELL_M_BH_$mytime.log"


#MAPS_3G_CN_D
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 9 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CN_D\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CN_D_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 9 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_D_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CN_D/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CN_D/ -re -rg "^Nigeria_BIB_3G_Country_Daily_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CN_D 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_D_$mytime.log"


#MAPS_3G_CN_D_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 10 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CN_D_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CN_D_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 10 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_D_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CN_D_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CN_D_BH/ -re -rg "^Nigeria_BIB_3G_Country_Daily_BH_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CN_D_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_D_BH_$mytime.log"


#MAPS_3G_CN_W_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 11 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CN_W_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CN_W_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 11 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CN_W_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CN_W_BH/ -re -rg "^Nigeria_BIB_3G_Country_Weekly_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CN_W_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_W_BH_$mytime.log"


#MAPS_3G_CN_M
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 12 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CN_M\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CN_M_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 12 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_M_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CN_M/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CN_M/ -re -rg "^Nigeria_BIB_3G_Country_Monthly_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CN_M 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_M_$mytime.log"


#MAPS_3G_CN_W
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 13 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_3G_CN_W\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_3G_CN_W_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 13 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_W_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_3G_CN_W/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_3G_CN_W/ -re -rg "^Nigeria_BIB_3G_Country_Weekly_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_3G_CN_W 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_3G_CN_W_$mytime.log"


#MAPS_2G_CN_M_DATA_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 14 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CN_M_DATA_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_2G_CN_M_DATA_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_2G_CN_M_DATA_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 14 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CN_M_DATA_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_2G_CN_M_DATA_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_2G_CN_M_DATA_BH/ -re -rg "^Nigeria_BIB_2G_Country_Monthly_Data_BH_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_2G_CN_M_DATA_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CN_M_DATA_BH_$mytime.log"


#MAPS_2G_CN_W_BH
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname \"$hostName\" --step 15 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_2G_CN_W_BH\""
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_MAPS_2G_CN_W_BH_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname \"$hostName\" --step 15 --log_directory \"/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CN_W_BH_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_2G_CN_W_BH/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_2G_CN_W_BH/ -re -rg "^Nigeria_BIB_2G_Country_Weekly_BH_([0-9]{8})([0-9]{2}).*zip$" -opp "|@1/@2/"  -nosim -lbl move_live_processed_arch_MAPS_2G_CN_W_BH 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_Maps_Feeds/move/${currDate}/move_live_proces_arch_MAPS_2G_CN_W_BH_$mytime.log"

#add new feeds
rm $PIDFILE

echo "Job: move_live_processed_to_archived. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 5m"
     emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
     ssh edge01002 " echo -e 'CronJob move processed to archived already running ....' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Move_Processed_To_Archived already running, attempt $i>' '$emailReceiver' " 
     ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status -1 --hostname \"$hostName\" --step 1 --brokers $kafkaHostList --topic $TopicName --error_message \"Move_Processed_To_Archived already running, attempt $i\""
     sleep 5m
else
     main_process
     break
fi
done
