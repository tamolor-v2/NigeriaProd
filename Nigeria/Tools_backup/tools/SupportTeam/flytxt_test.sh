#! /bin/bash

emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/SupportTeam/logs
#ListofFiles=$(ls /mnt/beegfs/tools/SupportTeam/data/*.csv)
filename=/mnt/beegfs/tools/SupportTeam/data/air_refill__$(date +"%Y%m%d%H").csv
filename1=air_refill__$(date +"%Y%m%d%H").csv
#######################################################################################

##SENDING START EMAIL

ssh edge01002 " echo -e 'CronJob Flytxt Refill Files Started at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Flytxt Refill Files Started at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: Flytxt Refill Files Status: START. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/logs_${currDate}_new.log


chmod 777  ${generalLogs}/logs_${currDate}_new.log


#######################################################################################

#The Query

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "SELECT concat ('234',accountnumber) as  MSISDN,original_timestamp_enrich as Transaction_Date, dedicatedaccountid,try_cast(case when length(accountbalance)=0 then 0.0 else try_cast(accountbalance as double) end as bigint) DA_137_bal_before_recharge_, try_cast(case when length(accountbalance____1 )=0 then 0.0 else try_cast(accountbalance____1  as double) end as bigint) DA_137_bal_after_recharge, try_cast(case when length(refilldivisionamount____1 )=0 then 0.0 else try_cast(refilldivisionamount____1  as double) end as bigint) Recharge_Amount from flare_8.CS5_AIR_REFILL_DA  where tbl_dt=$currDate and dedicatedaccountid=137  and length(accountbalance)=0"  --output-format TSV_HEADER > $filename

sed -i 's/\t/,/g' $filename
chmod 777 $filename

###########################################################################################

#COPYING INTO FLYTXT SERVER 


echo "Starting copy process to flytxt server" >> ${generalLogs}/logs_${currDate}_new.log

scp -p -i /mnt/beegfs/tools/SupportTeam/key.pem  /mnt/beegfs/tools/SupportTeam/data/$filename1   daas_user@10.1.204.41:/ftp-live/realtime/refill_im/

##chmod 777 /ftp-live/realtime/refill_im/$filename1 

echo "Successfully copy files to flytxt server: '$filename1' " >> ${generalLogs}/logs_${currDate}_new.log


############################################################################################

#Moving to Archive

mv /mnt/beegfs/tools/SupportTeam/data/$filename1 /mnt/beegfs/tools/SupportTeam/data/archive

echo "Successfully move files to archive '$filename1' " >> ${generalLogs}/logs_${currDate}_new.log

###################################################################################################

#SENDING FINISH EMAIL

ssh edge01002 " echo -e 'CronJob Flytxt Refill Files Finished at $(date +"%T") on edge01002\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<Flytxt Refill Files Finished at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: Flytxt Refill Files Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/logs_${currDate}_new.log

