#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/MoveFromlocal_crontab_lz.pid
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    exit 1
  fi
fi

currDate=$(date +"%Y%m%d")
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)

#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 44 --status 0 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName --general_message "start moveing data from local"
java -Xmx30g -Xms30g -Dlog4j.configurationFile=/nas/share05/tools/fileOps/log4j2.xml -jar /nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /data/data_lz/beegfs/live -out /nas/share05/production/movefromlocaltodfs_new/report_new/Exclude/${currDate}/ -mv /mnt/beegfs_api/live/ -od -of -tf 1 -op lineCount -nt 30 -dp 15 -re -rcm -rfl -nosim -effl "^.*\.(_COPYING_|tmp|TMP)$" -effl "^\..*" -edfl "UT_DUMP|SGSN|SDP_ADJ_DA|SDP_DMP_AC|MSC_DaaS|MA_MONITOR|EWP_DYA_ACCOUNT_HOLDER_DUMP|FLYTXT_CAMPAIGN_EVENTS_DAT|SDP_PAM_ALL|SDP_ADJ_AC|CCN_GPRS_AC|CCN_GPRS_DA|CCN_SMS_AC|CCN_SMS_DA|CCN_VOICE_AC|CCN_VOICE_DA|AIR_REFILL_AC|AIR_REFILL_DA|AIR_ADJ_DA|SMSC|CS6_Unified|DPI|MOBILE_MONEY|SDP_ADJ_MA|TMP_DMP_DUMP_ALL|SDP_DMP_MA|UDC_DUMP|SDP_DMP_DA|UC_DUMP|OFFER_DUMP|BUNDLE4U_GPRS|BUNDLE4U_VOICE|CCN_GPRS_MA|CCN_SMS_MA|AIR_ADJ_MA|AIR_REFILL_MA|CCN_VOICE_MA|MSC_CDR|NGVS_CDR|GPRS_MA_TEMP|MISSING_FILES_CCN|MVAS_DND_MSISDN_REP_CDR|LTE_HSS|ERS_VTU_DUMP|AVAYA_IVR|QRIOS_TRANSACTIONS|SPONSORED_DATA_MA|SPONSORED_DATA_DA|FLYTXT_INBOUND_EVENTS_DATA|APLIMAN_OBD|SIS_TOKEN|SMART_APP_DOWNLOAD|HSDP_DOL_LOG|MSO_MNP_NUMBER_SUMMARY" 2>&1 | tee "/mnt/beegfs_bsl/production/logs/movefromlocaltodfs_${currDate}_$mytime.log"


#numberOfFiles=$(tail -n 3 /mnt/beegfs/production/logs/movefromlocaltodfs_${currDate}_$mytime.log)

#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 44 --status 1 --log_directory "/mnt/beegfs/production/logs/movefromlocaltodfs_${currDate}_$mytime.log" --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName --general_message "${numberOfFiles}"
