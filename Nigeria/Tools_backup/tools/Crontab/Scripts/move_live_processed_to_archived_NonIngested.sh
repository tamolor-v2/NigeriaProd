PIDFILE=/home/daasuser/PIDFiles/move_live_processed_to_archived_NonIngested.pid
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




feed_name=CS6_CCN_CDR_VOICE
#SDPCDR_4001_OWSDPN1_SUB_0580_
#20181219084413_092459_CCN_SMS_ABSCP1_M.dat
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/CS6_Unified/CCN_CDR_VOICE/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/$feed_name/  -rg "^([0-9]{8})([0-9]{2}).*(CCN_VOICE).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_CS6_CCN_VOICE_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CS6_CCN_VOICE_$mytime.log"



feed_name=CS6_CCN_CDR_SMS
#SDPCDR_4001_OWSDPN1_SUB_0580_
#20181219084413_092459_CCN_SMS_ABSCP1_M.dat
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/CS6_Unified/CCN_CDR_SMS/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/$feed_name/  -rg "^([0-9]{8})([0-9]{2}).*(CCN_SMS).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_CS6_CCN_SMS_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_NonIngested_$mytime.log"


feed_name=CS6_CCN_CDR_GPRS
#20181201123553_03043_CCN_GPRS_OJOCCM_M.dat
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/CS6_Unified/CCN_CDR_GPRS/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/$feed_name/  -rg "^([0-9]{8})([0-9]{2}).*(CCN_GPRS).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_CS6_CCN_GPRS_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CS6_CCN_GPRS_$mytime.log"

feed_name=CS6_AIR_CDR
#20181223173651_8614_AIR_ASAIR18.dat
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/CS6_Unified/AIR_CDR/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/$feed_name/  -rg "^([0-9]{8})([0-9]{2}).*(_AIR_).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_CS6_AIR_CDR_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CS6_AIR_CDR_$mytime.log"


feed_name=CS6_SDP_CDR
#20181210030830_86018_SDP_ABSDPE1_M_ABSDPE1_SUB_5668.dat
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/CS6_Unified/SDP_CDR/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/$feed_name/  -rg "^([0-9]{8})([0-9]{2}).*(_SDP_).*$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_CS6_SDP_CDR_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_CS6_SDP_CDR_$mytime.log"

ime java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/SDP_LCY_MA /mnt/beegfs/live/SDP_SFD_MA  /mnt/beegfs/live/AIR_ERR_MA  /mnt/beegfs/live/SDP_NBL_MA -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(SDP_SFD_MA|AIR_ERR_MA|SDP_LCY_MA||SDP_NBL_MA)_.*$" -opp "|@3/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_all_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_NonIngested_$mytime.log"

feed_name=SDP_PAM_ALL
#SDPCDR_4001_OWSDPN1_SUB_0580_
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/SDP_PAM_ALL/incoming  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/$feed_name/  -rg "^.*([0-9]{8})-([0-9]{2}).*(ASN[.]PAM)$" -opp "|@1/|@2"  -nosim -lbl move_live_proces_arch_all_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_NonIngested_$mytime.log"


feed_name=TF_S2S_CS6_SDP_CDR
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/TF_S2S_CS6_SDP_CDR/  -out /mnt/beegfs/tools/Crontab/logs/move/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived -re -rg "^([0-9]{8})([0-9]{2}).*_(SDP)_.*$" -opp "|$feed_name/|@1/|@2" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_all_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_All_NonIngested_$mytime.log"

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
     sleep 5m
else
     main_process
     break
fi
done

