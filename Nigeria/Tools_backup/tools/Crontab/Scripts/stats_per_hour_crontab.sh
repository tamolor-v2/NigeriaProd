#!/bin/bash

today=`date +'%Y%m%d'`
yest=$(date -d '-1 day' '+%Y%m%d')
yest7=$(date -d '-7 day' '+%Y%m%d')
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
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 25 --status 0 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv\" --brokers $kafkaHostList --topic $TopicName"
echo "Job: stats_per_hour. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

mkdir -p /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest} 
rm -r /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${removeDir}
ssh edge01002 " echo -e 'CronJob \"stats_per_hours.sh\" Started at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Stats Per Day Hours for a week Started at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hive -e "msck repair table flare_8.file_stats"
hive -e "set tez.am.resource.memory.mb = 20048;set hive.tez.container.size=32096;set tez.runtime.io.sort.mb = 20090;set hive.cli.print.header=true;select c.tbl_dt, c.hour, d.number_of_received_files, c.number_of_done_files from (select b.tbl_dt, substring(b.endtime,12,2) as hour, count(distinct b.filename) number_of_done_files from flare_8.file_stats b where tbl_dt >= ${yest7}  and tbl_dt <= ${yest} group by tbl_dt, substring(b.endtime,12,2) ) c inner join (select a.tbl_dt, substring(a.starttime,12,2) as hour, count(distinct a.filename) number_of_received_files from flare_8.file_stats a where tbl_dt >= ${yest7}  and tbl_dt <= ${yest} group by tbl_dt, substring(a.starttime,12,2) ) d on c.tbl_dt = d.tbl_dt and c.hour = d.hour order by c.tbl_dt, c.hour"  | sed 's/[\t]/,/g'  > /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv
retVal=$?
if [ $retVal -eq 0 ];
then
ssh edge01002 " echo -e 'Hi\nPlease find attahced file which includes stats per day hours.\nThanks\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -a  /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv -s 'DAAS_Note_MTN_NG_<Stats Per Day Hours for a week Finished at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: stats_per_hour. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 25 --status 1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv\" --brokers $kafkaHostList --topic $TopicName"
else
ssh edge01002 " echo -e 'CronJob \"stats_per_hours.sh\" Failed at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Alert_NG@edge01001.mtn.com' -s 'DAAS_Alert_MTN_NG_<Stats Per Day Hours  Failed at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: stats_per_hour. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
ssh datanode01038 "bash /mnt/beegfs/tools/JsonBuilder/JsonBuilder.scala --id 25 --status -1 --hostname \"$hostName\" --step 1 --log_directory \"/mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv\" --brokers $kafkaHostList --topic $TopicName"
fi
