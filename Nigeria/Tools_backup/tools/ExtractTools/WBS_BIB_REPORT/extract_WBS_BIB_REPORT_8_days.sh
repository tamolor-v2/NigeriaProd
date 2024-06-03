#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

#ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
#kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
#TopicName=CentralMetaStore
#hostName=$(hostname)
#bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 16 --status 0 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName
### push to kafka here
yest=$(date -d '-1 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs_bsl/tools/Crontab/Scripts/email.dat)
currDate=$(date -d '-3 days' +"%Y%m%d")
echo "date: $currDate"
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
c=1
while [[ $c -le 8 ]]
do 
echo "processing : $currDate"
#Send Start Job Email
#ssh edge01002 "  echo -e 'CronJob \"extract_WBS_BIB_REPORT.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract WBS_BIB_REPORT Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
bash /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/wbsBIBReport.sh ${currDate}
retVal=$?
echo "ret2=$retVal"
if [ $retVal -eq 0 ];
then
#Send Job Finished Email
hadoop fs -mkdir -p /FlareData/output_8/WBS_BIB_REPORT/tbl_dt=${currDate}
echo "running: hadoop fs -rm /FlareData/output_8/WBS_BIB_REPORT/tbl_dt=${currDate}/*"
hadoop fs -rm /FlareData/output_8/WBS_BIB_REPORT/tbl_dt=${currDate}/*
hadoop fs -put /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/spool/wbs_bib_report_${currDate}.csv.gz /FlareData/output_8/WBS_BIB_REPORT/tbl_dt=${currDate}/
hive -e "msck repair table flare_8.WBS_BIB_REPORT"
echo "running: hadoop fs -put /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/spool/wbs_bib_report_${currDate} /FlareData/output_8/WBS_BIB_REPORT/tbl_dt=${currDate}/"
#echo "mv /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/spool/wbs_bib_report_${currDate}.csv.gz /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/old/"
#mv /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/spool/wbs_bib_report_${currDate}.csv.gz /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/old/
# extractedRecordsNo=$(cat /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/logs/$currDate/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
#ssh edge01002 " echo -e 'CronJob \"extract_AGL_CRM_LGA_MAP.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $currDate \n' | mailx -r 'DAAS_Note_NG@edge01002.mtn.com' -s 'DAAS_Note_MTN_NG_<extract WBS_BIB_REPORT Finished for $currDate with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")' '$emailReceiver' "
#echo "Job: extract_WBS_BIB_REPORT. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
 bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 16 --status 1 --hostname "$hostName" --step 1 --brokers $kafkaHostList --topic $TopicName
### push to kafka here

else
#ssh edge01002 " echo -e 'CronJob \"extract_WBS_BIB_REPORT.sh\" Failed at $(date +"%T"), for day: $currDate \n' | mailx -r 'DAAS_Alert_NG@edge01002.mtn.com' -s 'DAAS_Alert_MTN_NG_<extract WBS_BIB_REPORT Failed for $currDate at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: extract_WBS_BIB_REPORT. Status: Failed. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
 #bash /mnt/beegfs_bsl/tools/JsonBuilder/JsonBuilder.scala --id 16 --status -1 --hostname "$hostName" --step 1  --brokers $kafkaHostList --topic $TopicName
### push to kafka here
fi
done
echo "retval=$retVal"
