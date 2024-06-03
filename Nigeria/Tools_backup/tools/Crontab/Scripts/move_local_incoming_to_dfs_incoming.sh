PIDFILE=/home/daasuser/PIDFiles/move_live_processed_to_archived.pid
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

echo "Job: move_live_processed_to_archived. Status: Started. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_REFILL_MA|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CCN_GPRS_AC|CCN_GPRS_DA|CCN_GPRS_MA|CCN_SMS_AC|CCN_SMS_DA|CCN_SMS_MA|CCN_VOICE_AC|CCN_VOICE_DA|CCN_VOICE_MA|DMC_DUMP_ALL|EWP_FINANCIAL_LOG|GGSN|SDP_ACC_ADJ_AC|SDP_ADJ_DA|SDP_ACC_ADJ_MA|SDP_DMP_MA|SGSN|CALL_REASON|CALL_REASON_MONTHLY|NEWREG_BIOUPDT_POOL|AGL_CRM_COUNTRY_MAP|AGL_CRM_LGA_MAP|AGL_CRM_STATE_MAP)_.*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_all_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_$mytime.log"
#
#move Fin log feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/EWP_FINANCIAL_LOG/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/EWP_FINANCIAL_LOG -re -rg  "^financiallog-([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2" -iffl "^financiallog-201[7|8|9]" -nosim -lbl move_live_proces_arch_EWP_FINANCIAL_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_EWP_FINANCIAL_LOG_$mytime.log"

#move AIR_REFILL_DA
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/AIR_REFILL_DA/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/AIR_REFILL_DA -re -rg  "^([0-9]{8})([0-9]{2}).*_(AIR_REFILL_SubDA)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AIR_REFILL_DA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archAIR_REFILL_DA_$mytime.log"

#move BUNDLE4U_VOICE
#--------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/BUNDLE4U_VOICE/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/BUNDLE4U_VOICE -re -rg  "^([0-9]{8})([0-9]{2}).*_(CCN_VOICE_AP)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_BUNDLE4U_VOICE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_BUNDLE4U_VOICE_$mytime.log"

#move BUNDLE4U_GPRS
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/BUNDLE4U_GPRS/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/BUNDLE4U_GPRS -re -rg  "^([0-9]{8})([0-9]{2}).*_(CCN_GPRS)_.*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_BUNDLE4U_GPRS_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archBUNDLE4U_GPRS_$mytime.log"

#move MAPS_INV_4G
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_INV_4G/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_INV_4G -re -rg  "^Nigeria_BIB_4GCell_INV_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_MAPS_INV_4G_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MAPS_INV_4G_$mytime.log"

#move MAPS_INV_3G
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_INV_3G/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_INV_3G -re -rg  "^Nigeria_BIB_3GCell_INV_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_MAPS_INV_3G_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_3G_$mytime.log"

#move MAPS_INV_2G
#----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MAPS_INV_2G/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MAPS_INV_2G -re -rg  "^Nigeria_BIB_2GCell_INV_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_MAPS_INV_2G_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMAPS_INV_2G_$mytime.log"

#move DMC_DUMP_ALL
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/DMC_DUMP_ALL/processed -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 2 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/DMC_DUMP_ALL/ -re -rg  "^.*([0-9]{8})_DUMP.*$" -opp "|@1"  -nosim -lbl move_live_proces_archDMC_DUMP_ALL_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_DMC_DUMP_ALL_$mytime.log"

#GGSN
#-----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/GGSN/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(GGSN)[\.|_].*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_GGSN_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_GGSN_$mytime.log"

#MSC
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MSC_CDR/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(MSC)[\.|_].*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MSC_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MSC_$mytime.log"

#--SDP_ADJ_DA
#------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_ADJ_DA/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_ADJ_DA -re -rg "^([0-9]{8})([0-9]{2}).*_(SDP_ACC_ADJ_DA)[\.|_].*$" -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_SDP_ADJ_DA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_ADJ_DA_$mytime.log"

#--HSDP
#------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/HSDP_CDR/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/HSDP/ -re -rg "^([0-9]{8})([0-9]{2}).*$"  -opp "|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_HSDP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_HSDP_$mytime.log"

#SGSN
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SGSN/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(SGSN)[\.|_].*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_SGSN_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SGSN_$mytime.log"

#SDP_DMP_AC
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DMP_AC/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DMP_AC/ -re -rg ".*DUMP([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DMP_AC_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_AC_$mytime.log"

#SDP_DMP_MA
#----
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DMP_MA/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DMP_MA/ -re -rg ".*_([0-9]{8})([0-9]{2}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_SDP_DMP_MA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_MA_$mytime.log"

#SDP_DMP_DA
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DMP_DA/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DMP_DA/ -re -rg ".*DUMP([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DMP_DA_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DMP_MA_$mytime.log"


#SDP_DUMP_OFFER
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DUMP_OFFER/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DUMP_OFFER/ -re -rg ".*offer\.([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DUMP_OFFER_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_OFFER_$mytime.log"

#SDP_DUMP_SUBSCRIBER
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/SDP_DUMP_SUBSCRIBER/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/SDP_DUMP_SUBSCRIBER/ -re -rg ".*subscriber_offer\.([0-9]{8})([0-9]{2}).*$" -opp "|@1"  -nosim -lbl move_live_proces_arch_SDP_DUMP_SUBSCRIBER_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_SDP_DUMP_SUBSCRIBER_$mytime.log"

#RECON
#----------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/RECON/processed  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/ -re -rg "^(RECON)_.*_([0-9]{8}).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_RECON_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_RECON_$mytime.log"


#move PM_RATED feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/WBS_PM_RATED_CDRS/processed_2/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(WBS_PM_RATED_CDRS)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_PM_RATED_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_PM_RATED_LOG_$mytime.log"

#move CUG_ACCESS_FEES feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CUG_ACCESS_FEES/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CUG_ACCESS_FEES)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CUG_ACCESS_FEES_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CUG_ACCESS_LOG_$mytime.log"

#move CB_SERV_MAST_VIEW feed
#-----------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CB_SERV_MAST_VIEW/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CB_SERV_MAST_VIEW)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CB_SERV_MAST_VIEW_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CB_SERV_MAST_VIEW_LOG_$mytime.log"


#List processed files in live cluster
#--------------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/  -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -lbl listLive 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/list/${currDate}/list_live_$(mytime).log"

#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/samer_scripts/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /home/daasuser/samer_scripts/MOBILE_MONEY/processed/ -out /mnt/beegfs/tmp/waddah/move -od -tf 10 -nt 32 -dp 15 -of -op list -mv /home/daasuser/samer_scripts/archived/MOBILE_MONEY -re -rg  "^([0-9]{8})(DAILY_TRANSACTIONS).*$" -opp "|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MOBILE_MONEY_ 2>&1 | tee "/mnt/beegfs/tmp/waddah/move/move_live_proces_archMOBILE_MONEY_$mytime.log"

#move MOBILE_MONEY
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MOBILE_MONEY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MOBILE_MONEY -re -rg  "(?=[a-zA-Z_]*)([0-9]{8})(?=.*)"  -opp "|@1" -iffl "201[7|8|9]" -nosim -lbl move_live_proces_arch_MOBILE_MONEY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_archMOBILE_MONEY_$mytime.log"

#move CALL_REASON
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CALL_REASON/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(CALL_REASON)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CALL_REASON_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_LOG_$mytime.log"

#move CALL_REASON_MONTHLY
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/CALL_REASON_MONTHLY/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{6})_(CALL_REASON_MONTHLY)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_CALL_REASON_MONTHLY_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CALL_REASON_MONTHLY_LOG_$mytime.log"

#move MNP_PORTING_BROADCAST
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/MNP_PORTING_BROADCAST/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(MNP_PORTING_BROADCAST).*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MNP_PORTING_BROADCAST_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MNP_PORTING_BROADCAST_LOG_$mytime.log"

#move NEWREG_BIOUPDT_POOL
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/NEWREG_BIOUPDT_POOL/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(NEWREG_BIOUPDT_POOL)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_NEWREG_BIOUPDT_POOL__ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_NEWREG_BIOUPDT_POOL_LOG_$mytime.log"

#move MVAS_DND_MSISDN_REP_CDR
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/MVAS_DND_MSISDN_REP_CDR/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/MVAS_DND_MSISDN_REP_CDR -re -rg  "^DND_NUM_([0-9]{8})([0-9]{6}).*$" -opp "|@1" -iffl "^DND_NUM_201[7|8|9]" -nosim -lbl move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_MVAS_DND_MSISDN_REP_CDR_LOG_$mytime.log"

#move AGL_CRM_COUNTRY_MAP
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_COUNTRY_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_COUNTRY_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_COUNTRY_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_COUNTRY_MAP_LOG_$mytime.log"

#move AGL_CRM_LGA_MAP
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_LGA_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_LGA_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_LGA_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_LGA_MAP_LOG_$mytime.log"

#move AGL_CRM_STATE_MAP
#------------------
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/FlareData/CDR/AGL_CRM_STATE_MAP/processed/ -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg  "^([0-9]{8})_(AGL_CRM_STATE_MAP)_.*$" -opp "|@2/|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_AGL_CRM_STATE_MAP_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_AGL_CRM_STATE_MAP_LOG_$mytime.log"

rm $PIDFILE
#List incoming files in live cluster
#--------------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/  -out /mnt/beegfs/tmp/waddah/list -od -tf 10 -nt 32 -dp 15 -of -op list -lbl listIncomingLive 2>&1 | tee "/mnt/beegfs/tmp/waddah/list/list_live_Incoming$(mytime).log"

echo "Job: move_live_processed_to_archived. Status: Finished. Time: ${generalTime}" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 5m"
     echo -e "CronJob \"move processed to archived\" already running at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Move_Processed_To_Archived already running, attempt $i>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
     sleep 5m
else
     main_process
     break
fi
done
