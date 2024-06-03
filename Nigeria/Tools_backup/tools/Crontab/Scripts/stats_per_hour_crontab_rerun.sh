#!/bin/bash

today=`date +'%Y%m%d'`
yest=$1
yest7=$(date -d "$1 -7 day" "+%Y%m%d")
removeDir=$(date -d "$1 -60 day" "+%Y%m%d")
ts=`date +'%Y%m%d%H%M%S'`
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
echo "Job: stats_per_hour. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

mkdir -p /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest} 
rm -r /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${removeDir}
echo -e "CronJob \"stats_per_hours.sh\" Started at $(date +"%T") on edge01002\n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Stats Per Day Hours for a week Started at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"

kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
#echo "select tbl_dt, substring(endtime,12,2) as hour, count(distinct filename) number_of_file from flare_8.file_stats where tbl_dt >= ${yest7}  and tbl_dt <= ${yest} group by tbl_dt, substring(endtime,12,2) order by tbl_dt, hour"
###hive -e "set hive.cli.print.header=true;select tbl_dt, substring(endtime,12,2) as hour, count(distinct filename) number_of_files from flare_8.file_stats where tbl_dt >= ${yest7}  and tbl_dt <= ${yest} group by tbl_dt, substring(endtime,12,2) order by tbl_dt, hour" | sed 's/[\t]/,/g'  > /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv
hive -e "set tez.am.resource.memory.mb = 20048;set hive.tez.container.size=32096;set tez.runtime.io.sort.mb = 20090;set hive.cli.print.header=true;select c.tbl_dt, c.hour, d.number_of_received_files, c.number_of_done_files from (select b.tbl_dt, substring(b.endtime,12,2) as hour, count(distinct b.filename) number_of_done_files from flare_8.file_stats b where tbl_dt >= ${yest7}  and tbl_dt <= ${yest} group by tbl_dt, substring(b.endtime,12,2) ) c inner join (select a.tbl_dt, substring(a.starttime,12,2) as hour, count(distinct a.filename) number_of_received_files from flare_8.file_stats a where tbl_dt >= ${yest7}  and tbl_dt <= ${yest} group by tbl_dt, substring(a.starttime,12,2) ) d on c.tbl_dt = d.tbl_dt and c.hour = d.hour order by c.tbl_dt, c.hour"  | sed 's/[\t]/,/g'  > /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv 2>&1 | tee /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/log_$(date +"%Y-%m-%d %H:%M:%S").log
retVal=$?
if [ $retVal -eq 0 ];
then
echo -e "Hi\nPlease find attahced file which includes stats per day hours.\nThanks\n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -a  /mnt/beegfs/tools/Crontab/logs/stats_per_hours/${yest}/stats_${yest7}_${yest}.csv -s "DAAS_Note_MTN_NG_<Stats Per Day Hours  Finished at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "Job: stats_per_hour. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
else
echo -e "CronJob \"stats_per_hours.sh\" Failed at $(date +"%T") on edge01002\n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Stats Per Day Hours  Failed at $(date +"%Y-%m-%d %H:%M:%S")>" "$emailReceiver"
echo "Job: stats_per_hour. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
fi
