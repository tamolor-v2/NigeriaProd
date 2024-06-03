#!/bin/bash

today=`date +'%Y%m%d'`
removeDir=$(date -d '-60 day' '+%Y%m%d')
ts=`date +'%Y%m%d%H%M%S'`
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 26 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/create_partition/${today}/create_partition_${today}.log\" --brokers $kafkaHostList --topic $TopicName"
echo "Job: create_partitions. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

mkdir -p /mnt/beegfs/tools/Crontab/logs/create_partition/${today} 
rm -r /mnt/beegfs/tools/Crontab/logs/create_partition/${removeDir}
rm -r $generalLogs/${removeDir}
ssh edge01002 " echo -e 'CronJob \"create_partition.sh\" Started at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<create_partition for a week Started at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

bash /mnt/beegfs/tools/Crontab/Scripts/create_partition.sh 2>&1 | tee /mnt/beegfs/tools/Crontab/logs/create_partition/${today}/create_partition_${today}.log

ssh edge01002 " echo -e 'CronJob \"create_partition.sh\" Finished at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<create_partition.sh Finished at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: create_partitions. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 26 --status 1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/create_partition/${today}/create_partition_${today}.log\" --brokers $kafkaHostList --topic $TopicName"
