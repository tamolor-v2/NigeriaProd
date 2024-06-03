#!/bin/bash

PIDFILE=/home/daasuser/PIDFiles/MoveFromlocal_MOBILE_MONEY_crontab_lz.pid
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
mkdir -p /nas/share05/production/movefromlocaltodfs/report_new/MOBILE_MONEY_move/${currDate}
mkdir -p /nas/share05/production/logs/${currDate}
java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs_bsl/tools/fileOps/log4j2.xml -jar /mnt/beegfs_bsl/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /data/data_lz/beegfs/live/MOBILE_MONEY/incoming -out /mnt/beegfs_bsl/production/movefromlocaltodfs/report_new/MOBILE_MONEY_move/${currDate}/ -mv /nas/share05/FlareProd/Working/MOBILE_MONEY/Clean -od -of -tf 1 -nt 15 -dp 15   -re -rcm -rfl -nosim -effl "^.*\.(_COPYING_|tmp|TMP)$" -effl "^\..*" 2>&1 | tee "/nas/share05/production/logs/${currDate}/movefromlocaltodfs_MOBILE_MONEY_MOVE_${currDate}_$mytime.log"
#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 44 --status 0 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName --general_message "start moveing data from local"
mkdir -p /nas/share05/production/movefromlocaltodfs/report_new/MOBILE_MONEY/${currDate}
python3.6  /nas/share05/tools/DataCleaner/CleanLineFeedsInFields.py  -od /nas/share05/FlareProd/Working/MOBILE_MONEY/testdir  -id /nas/share05/FlareProd/Working/MOBILE_MONEY/Clean -im ".*"  -dm "|"  2>&1 | tee /nas/share05/tools/DataCleaner/log/MOBILE_MONEY_DataCleaner_log_$(date +%Y%m%d_%s).txt

mkdir -p /nas/share05/production/movefromlocaltodfs/report_new/MOBILE_MONEY_move_2/${currDate}
java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs_bsl/tools/fileOps/log4j2.xml -jar /mnt/beegfs_bsl/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /nas/share05/FlareProd/Working/MOBILE_MONEY/Clean -out /mnt/beegfs_bsl/production/movefromlocaltodfs/report_new/MOBILE_MONEY_move_2/${currDate}/ -mv /nas/share05/FlareProd/Working/MOBILE_MONEY/Clean_2 -od -of -tf 1 -nt 15 -dp 15   -re -rcm -rfl -nosim -effl "^.*\.(_COPYING_|tmp|TMP)$" -effl "^\..*" 2>&1 | tee "/nas/share05/production/logs/${currDate}/movefromlocaltodfs_MOBILE_MONEY_MOVE_${currDate}_$mytime.log"

mkdir -p /mnt/beegfs_bsl/production/movefromlocaltodfs_fixed/report_new/MOBILE_MONEY/${currDate}
java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs_bsl/tools/fileOps/log4j2.xml -jar /mnt/beegfs_bsl/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /nas/share05/FlareProd/Working/MOBILE_MONEY/testdir -out /mnt/beegfs_bsl/production/movefromlocaltodfs_fixed/report_new/MOBILE_MONEY/${currDate}/ -mv /nas/share05/FlareProd/Data/live/MOBILE_MONEY_UPDATE_2/inc_test -od -of -tf 1 -op lineCount -nt 15 -dp 15   -re -rcm -rfl -nosim -effl "^.*\.(_COPYING_|tmp|TMP)$" -effl "^\..*" 2>&1 | tee "/nas/share05/production/logs/${currDate}/movefromlocaltodfs_MOBILE_MONEY_${currDate}_$mytime.log"

#logdate=$(date +"%Y%m%d")
#bash /mnt/beegfs_bsl/tools/minitarringLists/mini_nas.sh "MOBILE_MONEY_orginal" "^.*([0-9]{8}).*$" "/mnt/beegfs_bsl/live/MOBILE_MONEY/Clean_2" "$logdate"  "#1/#1" "#1-#1"
#numberOfFiles=$(tail -n 3 /mnt/beegfs/production/logs/movefromlocaltodfs_${currDate}_$mytime.log)

#bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 44 --status 1 --log_directory "/mnt/beegfs/production/logs/movefromlocaltodfs_${currDate}_$mytime.log" --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName --general_message "${numberOfFiles}"