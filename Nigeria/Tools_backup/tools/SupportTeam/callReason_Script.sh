
currDate=$(date +"%Y%m%d")

emailReceiver=$(cat /mnt/beegfs/tools/Crontab/Scripts/email.dat)
yest=$(date -d '-1 day' '+%Y%m%d')
#Send Start Job Email

ssh edge01002 " echo -e 'CronJob \"nokiatt.sh\" Started at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<nokiatt.sh Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "
echo "Job: nokiatt.sh. Status: Started. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "select  case when closed_date is null or length(closed_date)=0 or closed_date='null' then '' else date_format(date_parse(lower(closed_date),'%d-%b-%Y %H:%i:%s'),'%d-%m-%Y %H:%i:%s') end OPEN_DATE, date_format(from_unixtime(open_date/1000),'%d-%m-%Y %H:%i:%s') CLOSED_DATE, SR_TT_NUMBER,MSISDN,CHANNEL,CUSTOMER_CONTACT,TT_TYPE,TT_PRIORITY ,AREA,SUB_AREA,STATUS,RETAIL_SHOP,CURRENT_LOCATION   from flare_8.call_reason where tbl_dt = ${yest}" --output-format CSV_HEADER | sed 's/[\t]/,/g' | awk '{gsub(/\"/,"")};1' >  /mnt/beegfs/ctma/NOKIACSI_TT_${yest}.csv;
#Send finish Job Email

ssh edge01002 " echo -e 'CronJob \"nokiatt.sh\" finished at $(date +"%T") on edge01001, for day: $yest \n' | mailx -r 'DAAS_Note_NG@edge01001.mtn.com' -s 'DAAS_Note_MTN_NG_<nokiatt.sh finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver' "

echo "Job: nokiatt.sh. Status: f. Time: $(date +"%Y-%m-%d %H:%M:%S")" >> ${generalLogs}/${currDate}/genral_logs_${currDate}.log

