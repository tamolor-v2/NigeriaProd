

curr_timestamp=$(date +"%Y:%m:%d %H:%M:%S")
yest=$(date -d '-1 day' '+%Y%m%d')
emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
currDate=$(date +"%Y%m%d")
generalTime=$(date +"%Y-%m-%d %H:%M:%S")

generalLogs=/mnt/beegfs/tools/SupportTeam/logs
sudo -u daasuser kdir ${generalLogs}/${currDate}/logs_${currDate}.log
ListofFiles=$(ls /mnt/beegfs/tools/SupportTeam/data/*.csv)

sudo -u daasuser echo "Start generate the main account balances of MSISDNs in service class 130 " >> ${generalLogs}/${currDate}/logs_${currDate}.log


/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "select account_balance , service_class_id , '$curr_timestamp' as date_time  from flare_8.sdp_dmp_ma where service_class_id='130' and tbl_dt = $yest "   --output-format CSV_HEADER > /mnt/beegfs/tools/SupportTeam/data/sdp_dmp_ma_130_$(date +"%Y%m%d%H").csv


sudo -u daasuser ssh datanode01038 " echo -e 'CrontJob generate the main account balances Started at $(date +"%T") on datanode01038\n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_
NG_< Generate the main account balances Started at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

sudo -u daasuser ssh datanode01038 " echo -e 'CronJob Generate the main account balances of MSISDNs in service class 130  Finished at $(date +"%T") on datanode01038\n' | mailx -r 'DAAS_Note_NG@datanode01038.mtn.com' -s 'DAAS_Note_MTN_
NG_< Generate the main account balances of MSISDNs in service class 130 Finished at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

sudo -u daasuser echo "Job: Lumous main account balances  Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/logs_${currDate}.log

