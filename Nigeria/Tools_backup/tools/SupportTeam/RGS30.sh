
currDate=$(date +"%Y%m%d")

emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)

yest=$(date -d '-1 day' '+%Y%m%d')

#Send Start Job Email

ssh edge01002 " echo -e 'CronJob \"RGS30.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<RGS30.sh Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "


echo "Job: RGS30.sh. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute " select msisdn_key from flare_8.customersubject where aggr='daily' and dola between 0 and 29 and datausage30dayskb =0  and tbl_dt =${yest} " --output-format CSV_HEADER | sed 's/[\t]/,/g' | awk '{gsub(/\"/,"")};1' > /mnt/beegfs/tools/SupportTeam/data/rgs/Dola30_OfferID${yest}.csv

scp /mnt/beegfs/tools/SupportTeam/data/rgs/*.csv daas_user@10.1.204.41:/DaaS_Flytxt_Data/Extracts/Extracts/Dola30_OfferID

mv /mnt/beegfs/tools/SupportTeam/data/rgs/*.csv /mnt/beegfs/tools/SupportTeam/data/archive
 chmod 777  /mnt/beegfs/tools/SupportTeam/data/archive/Dola30_OfferID${yest}.csv
chmod 777  /mnt/beegfs/tools/SupportTeam/data/rgs/Dola30_OfferID${yest}.csv

#Send finish Job Email

ssh edge01002 " echo -e 'CronJob \"RGS30.sh\" finished at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<RGS30.sh finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: RGS30.sh. Status: f. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log


