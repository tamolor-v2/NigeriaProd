#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yest=$1
######yest=`date +'%Y%m%d'`
removeDir=$(date -d '-60 day' '+%Y%m%d')
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/nas/share05/tools/Crontab/logs/general_logs
emailReceiver=$(cat /nas/share05/tools/Crontab/Scripts/email.dat)
echo "Job: extract_NEWREG_BIOUPDT_POOL. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.lo
ZookeeperHostList="datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181"
kafkaHostList="datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
TopicName=CentralMetaStore
hostName=$(hostname)
bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 0 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/logs/$yest/" --brokers $kafkaHostList --topic $TopicName
### push to kafka hereg
rm -r /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/logs/${removeDir}
#Send Start Job Email..
ssh edge01002 " echo -e 'CronJob \"extract_NEWREG_BIOUPDT_POOL.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Extract NEWREG_BIOUPDT_POOL Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "00 01 02 03 04 05 06 07 08 09" 
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "10 11 12 13 14 15 16 17 18 19" 
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "20 21 22 23 24 25 26 27 28 29"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "30 31 32 33 34 35 36 37 38 39"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "40 41 42 43 44 45 46 47 48 49"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "50 51 52 53 54 55 56 57 58 59"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "60 61 62 63 64 65 66 67 68 69"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "70 71 72 73 74 75 76 77 78 79"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "80 81 82 83 84 85 86 87 88 89"
bash /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/driver_NEWREG_BIOUPDT_POOL.sh /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/ $yest "90 91 92 93 94 95 96 97 98 99"

 mv /nas/share05/FlareProd/Working/NEWREG_BIOUPDT_POOL/${yest} /nas/share05/FlareProd/Data/live/NEWREG_BIOUPDT_POOL_WEEKLY/incoming/ 
#Send Job finished Email
extractedRecordsNo=$(cat /nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/logs/$yest/*.log | grep "summary: " | more | cut -d ',' -f 2 | cut -d ' ' -f 2 | awk '{total=total+$1} END {print total}')
ssh edge01002 " echo -e 'CronJob \"extract_NEWREG_BIOUPDT_POOL.sh\" Finished at $(date +"%T"), with $extractedRecordsNo extracted records, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<extract NEWREG_BIOUPDT_POOL Finished for $yest with $extractedRecordsNo extracted records at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: extract_NEWREG_BIOUPDT_POOL. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log
bash /nas/share05/tools/JsonBuilder/JsonBuilder.scala --id 5 --status 1 --hostname "$hostName" --step 1 --log_directory "/nas/share05/tools/ExtractTools/NEWREG_BIOUPDT_POOL/logs/$yest/" --general_message "$extractedRecordsNo extracted records" --brokers $kafkaHostList --topic $TopicName
### push to kafka here
