
#!/bin/bash
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
 
 tbl_dt_string=$(date +"%Y%m%d")
hour_string=$(date +"%H")
generalLogs=/mnt/beegfs/tools/Crontab/logs/general_logs
logDir=/mnt/beegfs/tools/Crontab/logs/incoming_analysis/tbl_dt=${tbl_dt_string}/hr=${hour_string}
mkdir -p ${logDir}
echo " start make directory on hdfs\n"
hdfs dfs -mkdir -p /FlareData/output_8/incoming_analysis/tbl_dt=${tbl_dt_string}/hr=${hour_string}
echo " directory ${tbl_dt_string}/${hour_string} created/n start identify file to put it on HDFS \n "
file_to_put=$(find /mnt/beegfs/tools/Crontab/listIncoming/${tbl_dt_string}/dir* -printf '%p\n' | sort -r | head -n 1)
echo " the file is ${file_to_put}/n start push it on hdfs \n  "
hdfs dfs -put $file_to_put /FlareData/output_8/incoming_analysis/tbl_dt=${tbl_dt_string}/hr=${hour_string}/

emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)


hive -e "msck repair table flare_8.incoming_analysis "
/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "select lxfiles ,path from incoming_analysis  where level =0 and tbl_dt =$tbl_dt_string and hr =$hour_string
 " --output-format CSV_HEADER >>/mnt/beegfs/tools/anaylsis_incoming/incoming_report/incoming_report_${tbl_dt_string}_${hour_string}.csv

#if [ll /mnt/beegfs/tools/anaylsis_incoming/incoming_report/incoming_report_${tbl_dt_string}_${hour_string}.csv |wc-l > 0]
#then 
#echo"remove partion hdfs://ngdaas/FlareData/output_8//incoming_analysis/tbl_dt=${tbl_dt_string}/hr=${hour_string} -1 "

 #hadoop fs -rm -r  hdfs://ngdaas/FlareData/output_8//incoming_analysis/tbl_dt=${tbl_dt_string}/hr=${hour_string} -1
#else 
#echo "the line count of file is zero "


#Send finish Job Email

ssh edge01002 " echo -e 'CronJob \"incoming_analysis.sh\" finished at $(date +"%T") on node38, for hour: $hour_string \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -a /mnt/beegfs/tools/incoming/incoming_report_${tbl_dt_string}_${hour_string}.csv -s 'DAAS_Note_MTN_NG_<incoming_analysis.sh finished for $hour_string at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: incoming_analysis.sh. Status: f. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${tbl_dt_string}/genral_logs_${tbl_dt_string}.log


