#List incoming files in live cluster
#--------------------------
PIDFILE=/home/daasuser/PIDFiles/list_file_incoming.pid
check_pid_file(){
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
  return 2
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
    return 2
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
  return 2
  fi
fi
}
main_process(){
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: list_live_incoming. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
today=$(date  '+%Y%m%d')
ts=`date +'%Y%m%d%H%M%S'`
datetime=`date +'%Y%m%d%H%M%S'`
removeDir=$(date -d '-60 day' '+%Y%m%d')
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 21 --status 0 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/Crontab/listIncoming/${today}" --brokers $kafkaHostList --topic $TopicName
mkdir -p /mnt/beegfs/tools/Crontab/listIncoming/${today}
rm -r /mnt/beegfs/tools/Crontab/listIncoming/${removeDir}
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in /mnt/beegfs/live/SDP_ADJ_MA/incoming /mnt/beegfs/live/SGSN/incoming /mnt/beegfs/live/MSC_CDR/incoming /mnt/beegfs/live/DMC_DUMP_ALL/incoming /mnt/beegfs/live/GGSN/incoming /mnt/beegfs/live/CCN_VOICE_MA/incoming /mnt/beegfs/live/CCN_VOICE_DA/incoming /mnt/beegfs/live/CCN_VOICE_AC/incoming /mnt/beegfs/live/CCN_SMS_DA/incoming /mnt/beegfs/live/CCN_GPRS_DA/incoming /mnt/beegfs/live/CCN_GPRS_AC/incoming /mnt/beegfs/live/AIR_REFILL_MA/incoming /mnt/beegfs/live/AIR_REFILL_AC/incoming /mnt/beegfs/live/AIR_ADJ_MA/incoming /mnt/beegfs/live/SDP_ADJ_DA/incoming /mnt/beegfs/live/SDP_ADJ_AC/incoming /mnt/beegfs/live/CCN_SMS_MA/incoming /mnt/beegfs/live/CCN_SMS_AC/incoming /mnt/beegfs/live/CCN_GPRS_MA/incoming /mnt/beegfs/live/BUNDLE4U_VOICE/incoming /mnt/beegfs/live/BUNDLE4U_GPRS/incoming /mnt/beegfs/live/AIR_REFILL_DA/incoming /mnt/beegfs/live/AIR_ADJ_DA/incoming /mnt/beegfs/live/SDP_DMP_MA/incoming /mnt/beegfs/live/RECON/incoming /mnt/beegfs/live/MOBILE_MONEY/incoming /mnt/beegfs/live/MAPS_INV_4G/incoming /mnt/beegfs/live/MAPS_INV_3G/incoming /mnt/beegfs/live/MAPS_INV_2G/incoming /mnt/beegfs/live/EWP_FINANCIAL_LOG/incoming /mnt/beegfs/live/HSDP_live/incoming /mnt/beegfs/live/MVAS_DND_MSISDN_REP_CDR/incoming /mnt/beegfs/live/SDP_DMP_DA/incoming /mnt/beegfs/live/LBN/incoming /mnt/beegfs/live/UDC_DUMP/incoming /mnt/beegfs/live/DPI/incoming  -out /mnt/beegfs/tools/Crontab/listIncoming/${today} -od -tf 20 -nt 32 -dp 5 -of -effl "^.*\.tmp$" -op list -lbl listIncomingLive 2>&1 | tee "/mnt/beegfs/tools/Crontab/listIncoming/${today}/list_live_Incoming${mytime}.log"

mv /mnt/beegfs/tools/Crontab/listIncoming/${today}/latest/* /mnt/beegfs/tools/Crontab/listIncoming/${today}

cd /mnt/beegfs/tools/incomingNumberCheck/script
bash /mnt/beegfs/tools/incomingNumberCheck/script/incomingNumberCheck.sh

rm $PIDFILE
echo "Job: list_live_incoming. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 21 --status 1 --hostname "$hostName" --step 1 --log_directory "/mnt/beegfs/tools/Crontab/listIncoming/${today}" --brokers $kafkaHostList --topic $TopicName
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 2m"
     emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
     ssh edge01002 " echo -e 'CronJob \"list live incoming\" Started list files at $(date +"%T") on edge01001 \n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<List_Live_Incoming already running, attempt $i>' '$emailReceiver' "
bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 21 --status -1 --hostname "$hostName" --step 1 --error_message "List_Live_Incoming already running, attempt $i" --brokers $kafkaHostList --topic $TopicName

     sleep 20m
else
     main_process
     break
fi
done
