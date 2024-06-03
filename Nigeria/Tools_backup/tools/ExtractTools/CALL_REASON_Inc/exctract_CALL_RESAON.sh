#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
#bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 0 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/CALL_REASON_Inc/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
yest=$(date -d '-1 day' '+%Y%m%d')
cd /nas/share05/tools/ExtractTools/CALL_REASON_Inc/
removeDir=$(date -d '-60 day' '+%Y%m%d')
emailReceiver=$(cat /nas/share05/tools/Crontab/Scripts/email.dat)
rm -r /nas/share05/tools/ExtractTools/CALL_REASON_Inc/logs/${removeDir}
mkdir -p /nas/share05/tools/Crontab/logs/general_logs/${yest}
#runDate=$(date +"%Y%m%d")
runTime=$(date +"%H%M%S")

#if [[ "$runTime" < "235959" ]]
#then 
#currDate=$yest
#echo "Running for yesterday"
#else 
currDate=$(date +"%Y%m%d")
echo "Running for today"
#fi
echo "running for $currDate"
echo "$currDate $runTime"
filename=/nas/share05/tools/ExtractTools/CALL_REASON_Inc/TMP_MAX_SQ/tmp_max_sq_${currDate}.dat
#currDate=$(date +"%Y%m%d")
max_seq_file_name=/nas/share05/tools/ExtractTools/CALL_REASON_Inc/staging/maxSeq_${currDate}.dat
if [ ! -f $max_seq_file_name ]
then
    echo "Creating maxSeq file for today: ${currDate}"
    echo "${currDate}000000">$max_seq_file_name
fi
sed -i '/^$/d' $max_seq_file_name
prevMaxSeq=$( tail -n 1 ${max_seq_file_name})
echo "${max_seq_file_name}     $prevMaxSeq"
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/nas/share05/tools/Crontab/logs/general_logs
find /mnt/beegfs_bsl/live/DB_extract_lz/CALL_REASON_Inc/incoming/ -type f -name '.*' -execdir sh -c 'mv -i "$0" "./${0#./.}"' {} \;
echo "Job: exctract_PM_RATED. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#Send Start Job Email
#ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
#maxSeq=$( tail -n 1 /nas/share05/tools/ExtractTools/CALL_REASON_Inc/staging/maxSeq_${today}.txt)
echo "select /*+ parallel 12 */ max(to_timestamp('${maxSeq}','yyyymmddhh24missFF')) from wbs_client.wbs_cdr_${currDate} where ANUM is not null"
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
export ORACLE_BASE=/usr/lib/oracle
#select  /*+ parallel 6 */ max(to_timestamp(process_date,'yyyymmddhh24missFF')) from wbs_client.wbs_cdr_${currDate} where ANUM is not null;
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_PIOUSER/Wr0ngT3yaGain#789@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ojgpdb2-scan.mtn.com.ng)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ema_p1)))'
set term off
set termout off
set echo off
set underline off
set colsep ','
set pages 40000
SET LONG 50000;
set trimout on
set trimspool on
set feedback off
set heading off
set headsep off
SET LINESIZE 30000
set LONGCHUNKSIZE 30000
set pagesize 0
set wrap off
spool $filename
SELECT TO_CHAR(MAX(DATE_TIME_STAMP- 0.25/24), 'yyyymmddhh24miss') FROM provident.MNP_PORTING_BROADCAST;
spool off
quit
EOF
currMaxSeq=$( tail -n 1 ${filename})
#maxSeq=$(/opt/presto/bin/presto   --server master01004:8099 --catalog hive5 --execute  "select coalesce((select date_format(max(date_parse(process_date,'%Y-%m-%d %H:%i:%s.%f')),'%Y%m%d%H%i%s') from flare_8.wbs_pm_rated_cdrs where tbl_dt=$currDate),'${yest}235959')"| tr -d '"')
#yest=$runDate
maxSeq=$prevMaxSeq
echo "max_seq=$maxSeq"
bash /nas/share05/tools/ExtractTools/CALL_REASON_Inc/INC_driver_localJar_20190417.sh /nas/share05/tools/ExtractTools/CALL_REASON_Inc /mnt/beegfs_bsl/FlareData/CDR/CALL_REASON_Inc/tmp/ $currDate  "" $maxSeq $currMaxSeq
echo "First Return code $retVal"
find /mnt/beegfs_bsl/live/DB_extract_lz/CALL_REASON_Inc/incoming/ -type f -name '.*' -execdir sh -c 'mv -i "$0" "./${0#./.}"' {} \;
sleep 65
retVal=$?
if [ $retVal -eq 0 ];
then
#split command
echo $currMaxSeq>>${max_seq_file_name}
rmdir /mnt/beegfs_bsl/FlareData/CDR/CALL_REASON_Inc/incoming/$currDate
rmdir /mnt/beegfs_bsl/live/DB_extract_lz/CALL_REASON_Inc/incoming/$currDate
mv /mnt/beegfs_bsl/FlareData/CDR/CALL_REASON_Inc/tmp/$currDate /mnt/beegfs_bsl/live/DB_extract_lz/CALL_REASON_Inc/incoming/
retVal=$?
echo "main-Return Value: $retVal"
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
extractedRecordsNo=$(cat /nas/share05/tools/ExtractTools/CALL_REASON_Inc/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@mtn.com' -s 'DAAS_Note_MTN_NG_<extract PM_RATED Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status 1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/CALL_REASON_Inc/logs/$yest/" --general_message "$extractedRecordsNo extracted records"  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
else
ssh edge01002 " echo -e 'ronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Alert_MTN_NG_<Move PM_RATED to incoming directory Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/CALL_REASON_Inc/logs/$yest/"  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
else
#ssh edge01002 " echo -e 'CronJob \"exctract_PM_RATED.sh\" Failed at $(date +"%T"), for day: $yest \n' | mailx -r 'DAAS_note_ng@mtn.com' -s 'DAAS_Alert_MTN_NG_<extract PM_RATED Failed for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: exctract_PM_RATED. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
#bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 2 --status -1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/CALL_REASON_Inc/logs/$yest/"  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi