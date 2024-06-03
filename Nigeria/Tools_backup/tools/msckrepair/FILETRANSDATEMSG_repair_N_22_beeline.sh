#!/bin/bash -e

#date=$1
date=$(date -d '-1 day' '+%Y%m%d')
echo "Getting all feeds from FileTransDateMsg"

commands=$(hadoop fs -ls hdfs://ngdaas/FlareData/output_8/FileTransDateMsg/ |  awk '{if(NR>1)print}' | awk '{print $8}' | awk -F"." '{print "ALTER TABLE FLARE_8.FILETRANSDATEMSG ADD PARTITION (msgtype='\''com.mtn.messages."$4"'\'',file_date='\'''$date''\'') location '\''FlareData/output_8/FileTransDateMsg/msgtype=com.mtn.messages."$4"/file_date='$date''\'';"}')

echo "repairing the table..."
# echo $commands
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "$commands"

echo "Done"
