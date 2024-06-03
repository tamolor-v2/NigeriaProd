PIDFILE=/home/daasuser/PIDFiles/move_live_processed_to_archived.pid
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
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
#move All_Processed_feeds
mkdir -p /mnt/beegfs/tools/Crontab/logs/move/${currDate}
rm -r /mnt/beegfs/tools/Crontab/logs/move/${removeDir}
mkdir -p  ${generalLogs}/${currDate}
echo "Job: move_live_processed_to_archived. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}\" --brokers $kafkaHostList --topic $TopicName --general_message \"move all feeds\""

mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_REFILL_MA|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CCN_GPRS_AC|CCN_GPRS_DA|CCN_GPRS_MA|CCN_SMS_AC|CCN_SMS_DA|CCN_SMS_MA|CCN_VOICE_AC|CCN_VOICE_DA|CCN_VOICE_MA|DMC_DUMP_ALL|EWP_FINANCIAL_LOG|GGSN|SDP_ACC_ADJ_AC|SDP_ADJ_DA|SDP_ACC_ADJ_MA|SDP_DMP_MA|SGSN|CALL_REASON|CALL_REASON_MONTHLY|NEWREG_BIOUPDT_POOL|AGL_CRM_COUNTRY_MAP|AGL_CRM_LGA_MAP|AGL_CRM_STATE_MAP|UDC_DUMP|SDP_DMP_DA|LBN|NEWREG_BIOUPDT_POOL_WEEKLY|NEWREG_BIOUPDT_POOL_DAILY)_.*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_all_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_$mytime.log)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move most of feeds: $numberOfFiles\""

mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 2 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_EWP_FINANCIAL_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move fin_log\""

#
#move Fin log feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/EWP_FINANCIAL_LOG/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/EWP_FINANCIAL_LOG -re -rg  "^financiallog-([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2" -iffl "^financiallog-201[7|8|9]" -nosim -lbl move_live_proces_arch_EWP_FINANCIAL_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_EWP_FINANCIAL_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_EWP_FINANCIAL_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 2 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_EWP_FINANCIAL_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move fin_log: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archAIR_REFILL_DA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AIR_REFILL_DA\""
#move AIR_REFILL_DA
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/AIR_REFILL_DA/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/AIR_REFILL_DA -re -rg  "^([0-9]{8})([0-9]{2}).*_(AIR_REFILL_SubDA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AIR_REFILL_DA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archAIR_REFILL_DA_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archAIR_REFILL_DA_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 3 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archAIR_REFILL_DA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AIR_REFILL_DA: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_BUNDLE4U_VOICE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move BUNDLE4U_VOICE\""
#move BUNDLE4U_VOICE
#--------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/BUNDLE4U_VOICE/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/BUNDLE4U_VOICE -re -rg  "^([0-9]{8})([0-9]{2}).*_(CCN_VOICE_AP)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_BUNDLE4U_VOICE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_BUNDLE4U_VOICE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_BUNDLE4U_VOICE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 4 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_BUNDLE4U_VOICE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move BUNDLE4U_VOICE: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 5 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archBUNDLE4U_GPRS_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move BUNDLE4U_GPRS\""
#move BUNDLE4U_GPRS
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/BUNDLE4U_GPRS/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/BUNDLE4U_GPRS -re -rg  "^([0-9]{8})([0-9]{2}).*_(CCN_GPRS)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_BUNDLE4U_GPRS_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archBUNDLE4U_GPRS_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archBUNDLE4U_GPRS_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 5 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archBUNDLE4U_GPRS_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move BUNDLE4U_GPRS: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 6 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MAPS_INV_4G_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_INV_4G\""
#move MAPS_INV_4G
#----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_INV_4G/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_INV_4G -re -rg  "^Nigeria_BIB_4GCell_INV_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_MAPS_INV_4G_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MAPS_INV_4G_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MAPS_INV_4G_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 6 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MAPS_INV_4G_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_INV_4G: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 7 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_3G_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_INV_3G\""
#move MAPS_INV_3G
#----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_INV_3G/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_INV_3G -re -rg  "^Nigeria_BIB_3GCell_INV_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_MAPS_INV_3G_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_3G_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_3G_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 7 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_3G_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_INV_3G: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 8 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_2G_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_INV_4G\""
#move MAPS_INV_2G
#----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_INV_2G/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_INV_2G -re -rg  "^Nigeria_BIB_2GCell_INV_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_MAPS_INV_2G_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_2G_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_2G_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 8 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_2G_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MAPS_INV_4G: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 9 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_DMC_DUMP_ALL_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move DMC_DUMP_ALL\""
#move DMC_DUMP_ALL
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/DMC_DUMP_ALL/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 2 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/DMC_DUMP_ALL/ -re -rg  "^.*([0-9]{8})_DUMP.*$" -opp "|@1"  -nosim -lbl move_live_proces_archDMC_DUMP_ALL_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_DMC_DUMP_ALL_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_DMC_DUMP_ALL_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 9 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_DMC_DUMP_ALL_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move DMC_DUMP_ALL: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 10 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_GGSN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move GGSN\""
#GGSN
#-----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/GGSN/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(GGSN)[\.|_].*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_GGSN_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_GGSN_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_GGSN_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 10 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_GGSN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move GGSN: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 11 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MSC\""
#MSC
#----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MSC_CDR/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(MSC)[\.|_].*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MSC_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 11 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MSC: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 12 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_ADJ_DA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_ADJ_DA\""

#--SDP_ADJ_DA
#------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_ADJ_DA/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_ADJ_DA -re -rg "^([0-9]{8})([0-9]{2}).*_(SDP_ACC_ADJ_DA)[\.|_].*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_SDP_ADJ_DA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_ADJ_DA_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_ADJ_DA_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 12 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_ADJ_DA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_ADJ_DA: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 13 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_HSDP_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move HSDP\""
#--HSDP
#------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/HSDP_CDR/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/HSDP/ -re -rg "^([0-9]{8})([0-9]{2}).*$"  -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_HSDP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_HSDP_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_HSDP_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 13 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_HSDP_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move HSDP: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 14 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SGSN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SGSN\""
#SGSN
#----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SGSN/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(SGSN)[\.|_].*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_SGSN_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SGSN_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SGSN_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 14 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SGSN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SGSN: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 15 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_AC_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SGSN\""
#SDP_DMP_AC
#----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DMP_AC/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DMP_AC/ -re -rg ".*DUMP([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DMP_AC_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_AC_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_AC_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 15 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_AC_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SGSN: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 16 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_MA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DMP_MA\""

#SDP_DMP_MA
#----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DMP_MA/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DMP_MA/ -re -rg ".*_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_SDP_DMP_MA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_MA_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_MA_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 16 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_MA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DMP_MA: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 17 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_DA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DMP_DA\""

#SDP_DMP_DA
#----------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DMP_DA/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DMP_DA/ -re -rg ".*DUMP([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DMP_DA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_DA_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_DA_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 17 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_DA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DMP_DA: $numberOfFiles\""

mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 18 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_OFFER_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DUMP_OFFER\""

#SDP_DUMP_OFFER
#----------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DUMP_OFFER/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DUMP_OFFER/ -re -rg ".*offer\.([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DUMP_OFFER_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_OFFER_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_OFFER_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 18 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_OFFER_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DUMP_OFFER: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 19 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_SUBSCRIBER_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DUMP_SUBSCRIBER\""

#SDP_DUMP_SUBSCRIBER
#----------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DUMP_SUBSCRIBER/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DUMP_SUBSCRIBER/ -re -rg ".*subscriber_offer\.([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DUMP_SUBSCRIBER_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_SUBSCRIBER_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_SUBSCRIBER_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 19 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_SUBSCRIBER__$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DUMP_SUBSCRIBER: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 20 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move//${currDate}/move_live_proces_arch_RECON_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move RECON\""
#RECON
#----------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/RECON/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/ -re -rg "^(RECON)_.*_([0-9]{8}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_RECON_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_RECON_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_RECON_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 20 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_RECON_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_DUMP_SUBSCRIBER: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 21 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_PM_RATED_LOG__$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move PM_RATED\""
#move PM_RATED feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/WBS_PM_RATED_CDRS/processed_2/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(WBS_PM_RATED_CDRS)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_PM_RATED_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_PM_RATED_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_PM_RATED_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 21 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_PM_RATED_LOG__$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move PM_RATED: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 22 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CUG_ACCESS_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CUG_ACCESS_FEES\""

#move CUG_ACCESS_FEES feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CUG_ACCESS_FEES/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CUG_ACCESS_FEES)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CUG_ACCESS_FEES_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CUG_ACCESS_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CUG_ACCESS_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 22 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CUG_ACCESS_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CUG_ACCESS_FEES: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 23 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CB_SERV_MAST_VIEW_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CB_SERV_MAST_VIEW\""

#move CB_SERV_MAST_VIEW feed
#-----------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CB_SERV_MAST_VIEW/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CB_SERV_MAST_VIEW)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CB_SERV_MAST_VIEW_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CB_SERV_MAST_VIEW_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CB_SERV_MAST_VIEW_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 23 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CB_SERV_MAST_VIEW_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move  CB_SERV_MAST_VIEW: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 24 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMOBILE_MONEY_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MOBILE_MONEY\""

#List processed files in live cluster
#--------------------------
##mytime=$(date +"%Y-%m-%d_%H-%M-%S")
###time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/  -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -lbl listLive 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/list/${currDate}/list_live_$(mytime).log"

#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /home/daasuser/samer_scripts/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /home/daasuser/samer_scripts/MOBILE_MONEY/processed/ -out /mnt/beegfs/tmp/waddah/move -od -tf 10 -nt 32 -dp 15 -of -op list -mv /home/daasuser/samer_scripts/archived/MOBILE_MONEY -re -rg  "^([0-9]{8})(DAILY_TRANSACTIONS).*$" -opp "|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MOBILE_MONEY_ 2>&1 | tee "/mnt/beegfs/tmp/waddah/move/move_live_proces_archMOBILE_MONEY_$mytime.log"

#move MOBILE_MONEY
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MOBILE_MONEY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MOBILE_MONEY -re -rg  "(?=[a-zA-Z_]*)([0-9]{8})(?=.*)"  -opp "|@1" -iffl "201[7|8|9]" -nosim -lbl move_live_proces_arch_MOBILE_MONEY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMOBILE_MONEY_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMOBILE_MONEY_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 24 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMOBILE_MONEY_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MOBILE_MONEY: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 25 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CALL_REASON\""
#move CALL_REASON
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CALL_REASON/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CALL_REASON)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CALL_REASON_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 25 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CALL_REASON: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 26 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_MONTHLY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CALL_REASON_MONTHLY\""
#move CALL_REASON_MONTHLY
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CALL_REASON_MONTHLY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{6})_(CALL_REASON_MONTHLY)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CALL_REASON_MONTHLY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_MONTHLY_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_MONTHLY_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 26 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_MONTHLY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CALL_REASON_MONTHLY: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 27 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MNP_PORTING_BROADCAST_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MNP_PORTING_BROADCAST\""
#move MNP_PORTING_BROADCAST
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/MNP_PORTING_BROADCAST/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(MNP_PORTING_BROADCAST).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MNP_PORTING_BROADCAST_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MNP_PORTING_BROADCAST_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MNP_PORTING_BROADCAST_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 27 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MNP_PORTING_BROADCAST_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MNP_PORTING_BROADCAST: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 28 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_LOG__$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL\""
#move NEWREG_BIOUPDT_POOL
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(NEWREG_BIOUPDT_POOL)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL__ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 28 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_LOG__$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 29 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MVAS_DND_MSISDN_REP_CDR\""

#move MVAS_DND_MSISDN_REP_CDR
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MVAS_DND_MSISDN_REP_CDR/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MVAS_DND_MSISDN_REP_CDR -re -rg  "^DND_NUM_([0-9]{8})([0-9]{6}).*$" -opp "|@1" -iffl "^DND_NUM_201[7|8|9]" -nosim -lbl move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 29 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MVAS_DND_MSISDN_REP_CDR: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 30 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_COUNTRY_MAP_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP\""

#move AGL_CRM_COUNTRY_MAP
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_COUNTRY_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_COUNTRY_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_COUNTRY_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_COUNTRY_MAP_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_COUNTRY_MAP_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 30 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_COUNTRY_MAP_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_COUNTRY_MAP: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 31 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_LGA_MAP_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_LGA_MAP\""

#move AGL_CRM_LGA_MAP
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_LGA_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_LGA_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_LGA_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_LGA_MAP_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_LGA_MAP_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 31 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_LGA_MAP_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_LGA_MAP: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 32 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_STATE_MAP_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_STATE_MAP\""

#move AGL_CRM_STATE_MAP
#------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_STATE_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_STATE_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_STATE_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_STATE_MAP_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_STATE_MAP_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 32 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_STATE_MAP_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_CRM_STATE_MAP: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 33 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_UDC_DUMP_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move UDC_DUMP\""
#UDC_DUMP
#-----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/UDC_DUMP/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/UDC_DUMP -re -rg "^.*([0-9]{8}).*$" -opp "|@1" -iffl "201[7|8|9]" -nosim -lbl move_live_proces_arch_UDC_DUMP 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_UDC_DUMP_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_UDC_DUMP_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 33 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_UDC_DUMP_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move UDC_DUMP: $numberOfFiles\""
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#LBN
#-----
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 34 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_LBN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move LBN\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/LBN/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/LBN -re -rg "^([0-9]{8})([0-9]{2}).*_(TNP)$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_LBN 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_LBN_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_LBN_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 34 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_LBN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move LBN: $numberOfFiles\""


#move NEWREG_BIOUPDT_POOL_DAILY
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 35 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL_DAILY\""


time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL_DAILY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CB_NEWREG_BIOUPDT_POOL_DAILY).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 35 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_DAILY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL_DAILY: $numberOfFiles\""


#move NEWREG_BIOUPDT_POOL_WEEKLY
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 36 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL_WEEKLY\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL_WEEKLY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(NEWREG_BIOUPDT_POOL_WEEKLY).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY__ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY_LOG_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY_LOG_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 36 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_WEEKLY_LOG_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move NEWREG_BIOUPDT_POOL_WEEKLY: $numberOfFiles\""

#CIS_CDR
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 37 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CIS_CDR_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CIS_CDR\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/CIS_CDR/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/CIS_CDR/ -re -rg "^.*([0-9]{2})([0-9]{2})([0-9]{4})_([0-9]{2}).*$" -opp "|@3|@2|@1/|@4"  -nosim -lbl move_live_proces_arch_CIS_CDR 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CIS_CDR_$mytime.log"
numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CIS_CDR_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 37 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CIS_CDR_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CIS_CDR: $numberOfFiles\""

#MSC_DaaS
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 38 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_DaaS_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MSC_DaaS\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MSC_DaaS/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MSC_DaaS/ -re -rg "^.*_([0-9]{6})([0-9]{2}).*$" -opp "20|@1/|@2"  -nosim -lbl move_live_proces_arch_MSC_DaaS 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_DaaS_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_DaaS_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 38 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_DaaS_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MSC_DaaS: $numberOfFiles\""

#VSCDR_0001_APNGVS02_NGVS_12275_20181113-150845.json.csv
#NGVS
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 39 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_FLY_TXT_CAMPAIGN_EVENTS_DATA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move FLY_TXT_CAMPAIGN_EVENTS_DATA\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/FLY_TXT_CAMPAIGN_EVENTS_DATA/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/FLY_TXT_CAMPAIGN_EVENTS_DATA/ -re -rg "^([0-9]{8})_.*$" -opp "|@1/"  -nosim -lbl move_live_proces_arch_FLY_TXT_CAMPAIGN_EVENTS_DATA_ 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_FLY_TXT_CAMPAIGN_EVENTS_DATA_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_FLY_TXT_CAMPAIGN_EVENTS_DATA_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 39 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_FLY_TXT_CAMPAIGN_EVENTS_DATA_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move FLY_TXT_CAMPAIGN_EVENTS_DATA: $numberOfFiles\""

#DPI_CDR
#----------------
##mytime=$(date +"%Y-%m-%d_%H-%M-%S")
##time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/DPI_CDR/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/DPI_CDR -re -rg  "^DPI-([0-9]{8})-([0-9]{2})-.*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_DPI_CDR_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_DPI_CDR_$mytime.log"
mytime=$(date +"%Y-%m-%d_%H-%M-%S")

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 40 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_TRAFFIC_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move USSD_TRAFFIC_SUCCESS_RATE\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/USSD_TRAFFIC_SUCCESS_RATE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/USSD_TRAFFIC_SUCCESS_RATE -re -rg "^USSD_Traffic_Success_Rate_Per_Short_Code.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_USSD_TRAFFIC_SUCCESS_RATE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_TRAFFIC_SUCCESS_RATE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_TRAFFIC_SUCCESS_RATE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 40 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_TRAFFIC_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move USSD_TRAFFIC_SUCCESS_RATE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 41 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_ERROR_CODE_BREAKDOWN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move USSD_ERROR_CODE_BREAKDOWN\""


time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/USSD_ERROR_CODE_BREAKDOWN/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/USSD_ERROR_CODE_BREAKDOWN -re -rg "^USSD_Error_Code_Breakdown_Per_Short_Code.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_USSD_ERROR_CODE_BREAKDOWN_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_ERROR_CODE_BREAKDOWN_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_ERROR_CODE_BREAKDOWN_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 41 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_ERROR_CODE_BREAKDOWN_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move USSD_ERROR_CODE_BREAKDOWN: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 42 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_LICENSE_USAGE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move USSD_LICENSE_USAGE\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/USSD_LICENSE_USAGE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/USSD_LICENSE_USAGE -re -rg "^USSD_License_Report.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_USSD_LICENSE_USAGE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_LICENSE_USAGE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_LICENSE_USAGE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 42 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_USSD_LICENSE_USAGE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move USSD_LICENSE_USAGE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 43 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_SUBMIT_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_TOTAL_SUBMIT_SUCCESS_RATE\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SMSC_TOTAL_SUBMIT_SUCCESS_RATE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SMSC_TOTAL_SUBMIT_SUCCESS_RATE -re -rg "^SMSC_Total_Submit_Success_Rate_Report.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_SMSC_TOTAL_SUBMIT_SUCCESS_RATE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_SUBMIT_SUCCESS_RATE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_SUBMIT_SUCCESS_RATE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 43 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_SUBMIT_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_TOTAL_SUBMIT_SUCCESS_RATE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 44 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_TOTAL_DELIVERY_SUCCESS_RATE\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SMSC_TOTAL_DELIVERY_SUCCESS_RATE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SMSC_TOTAL_DELIVERY_SUCCESS_RATE -re -rg "^SMSC_Total_Delivery_Success_Rate_Report.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 44 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_TOTAL_DELIVERY_SUCCESS_RATE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 45 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE -re -rg "^SMSC_A2P_P2P_P2A_A2A_Success_Rate_Report.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 45 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 46 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_ERROR_BREAKDOWN_PER_ACCOUNT\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SMSC_ERROR_BREAKDOWN_PER_ACCOUNT/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SMSC_ERROR_BREAKDOWN_PER_ACCOUNT -re -rg "^SMSC_Error_Code_Breakdown_Per_Account.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$" -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 46 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_ERROR_BREAKDOWN_PER_ACCOUNT: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 47 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_ERROR_BREAKDOWN_PER_ACCOUNT\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SMSC_LICENSE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SMSC_LICENSE -re -rg "^SMSC_License_Report.*_([0-9]{4})-([0-9]{2})-([0-9]{2})_([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_SMSC_LICENSE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_LICENSE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_LICENSE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 47 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SMSC_LICENSE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SMSC_LICENSE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 48 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SCREAM_SERVICE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SCREAM_SERVICE\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SCREAM_SERVICE/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SCREAM_SERVICE -re -rg "^.*_([0-9]{8})([0-9]{2}).*$"  -opp "|@1/|@2" -nosim -lbl move_live_proces_arch_SCREAM_SERVICE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SCREAM_SERVICE_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SCREAM_SERVICE_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 48 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SCREAM_SERVICE_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SCREAM_SERVICE: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 49 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SAG_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SAG_API\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SAG_API/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SAG_API -re -rg "^.*_([0-9]{8})([0-9]{2}).*$"  -opp "|@1/|@2" -nosim -lbl move_live_proces_arch_SAG_API_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SAG_API_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SAG_API_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 49 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SAG_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SAG_API: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 50 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_IB_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move IB_API\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/IB_API/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/IB_API -re -rg "^.*_([0-9]{8})([0-9]{2}).*$"  -opp "|@1/|@2" -nosim -lbl move_live_proces_arch_IB_API_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_IB_API_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_IB_API_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 50 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_IB_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move IB_API: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 51 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CGW_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CGW_API\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/CGW_API/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/CGW_API -re -rg "^.*_([0-9]{8})([0-9]{2}).*$"  -opp "|@1/|@2" -nosim -lbl move_live_proces_arch_CGW_API_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CGW_API_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CGW_API_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 51 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CGW_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move CGW_API: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 52 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MyMTNApp_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MyMTNApp\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MyMTNApp/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MyMTNApp -re -rg "^scng_genericplugin_node.*([0-9]{4})-([0-9]{2})-([0-9]{2})-([0-9]{2}).*$"  -opp "|@1|@2|@3/|@4" -nosim -lbl move_live_proces_arch_MyMTNApp_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MyMTNApp_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MyMTNApp_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 52 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MyMTNApp_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MyMTNApp: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 53 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSO_PROCESS_AVG_TIME_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MSO_PROCESS_AVG_TIME\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/MSO_PROCESS_AVG_TIME/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MSO_PROCESS_AVG_TIME -re -rg "^([0-9]{8})_(MSO_PROCESS_AVG_TIME).*$"  -opp "|@1/" -nosim -lbl move_live_proces_arch_MSO_PROCESS_AVG_TIME_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSO_PROCESS_AVG_TIME_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSO_PROCESS_AVG_TIME_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 53 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSO_PROCESS_AVG_TIME_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move MSO_PROCESS_AVG_TIME: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 54 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_BILL_ANALYZER_MONTHLY_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_BILL_ANALYZER_MONTHLY\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_BILL_ANALYZER_MONTHLY/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/AGL_BILL_ANALYZER_MONTHLY -re -rg "^([0-9]{6})_(AGL_BILL_ANALYZER_MONTHLY).*$"  -opp "|@101/" -iffl "201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_BILL_ANALYZER_MONTHLY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_BILL_ANALYZER_MONTHLY_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_BILL_ANALYZER_MONTHLY_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 54 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_BILL_ANALYZER_MONTHLY_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move AGL_BILL_ANALYZER_MONTHLY: $numberOfFiles\""

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 55 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_XML_FILES_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move XML_FILES\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/XML_FILES/processed/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/XML_FILES -re -rg "^([0-9]{8})_(XML_FILES).*$"  -opp "|@1/" -nosim -lbl move_live_proces_arch_XML_FILES_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_XML_FILES_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_XML_FILES_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 55 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_XML_FILES_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move XML_FILES: $numberOfFiles\""

## move original sdp_api archived to structured archived
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 56 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_arch_origin_str_SDP_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_API\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/archived/SDP_API  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_API_str -re -rg "^sag_sag_interface_.*([0-9]{8})([0-9]{2})([0-9]{7}).*$" -opp "|@1/|@2" -nosim -lbl move_arch_orgin_str_SDP_API_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_arch_origin_str_SDP_API_$mytime.log"

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_arch_origin_str_SDP_API_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 56 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_arch_origin_str_SDP_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_API: $numberOfFiles\""

## move original files
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 0 --hostname \"$hostName\" --step 57 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_origin_str_SDP_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_API\""

time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/SDP_API/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/live/SDP_API/incoming_str -re -rg "^sag_sag_interface_.*([0-9]{8})([0-9]{2})([0-9]{7}).*$" -opp "|@1/|@2" -nosim -lbl move_live_orgin_str_SDP_API_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_origin_str_SDP_API_$mytime.log" 

numberOfFiles=$(tail -n 3 /mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_origin_str_SDP_API_$mytime.log)

ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 29 --status 1 --hostname \"$hostName\" --step 57 --log_directory \"/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_origin_str_SDP_API_$mytime.log\" --brokers $kafkaHostList --topic $TopicName --general_message \"move SDP_API: $numberOfFiles\""

rm $PIDFILE
#List incoming files in live cluster
#--------------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/  -out /mnt/beegfs/tmp/waddah/list -od -tf 10 -nt 32 -dp 15 -of -op list -lbl listIncomingLive 2>&1 | tee "/mnt/beegfs/tmp/waddah/list/list_live_Incoming$(mytime).log"

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