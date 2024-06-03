yest=$(date -d '-1 day' '+%Y%m%d')

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "select  date_format(from_unixtime(open_date/1000),'%d-%m-%Y %H:%i:%s') OPEN_DATE ,case when closed_date is null or length(closed_date)=0 or closed_date='null' then '' else date_format(date_parse(lower(closed_date),'%d-%b-%Y %H:%i:%s'),'%d-%m-%Y %H:%i:%s') end CLOSED_DATE, SR_TT_NUMBER,MSISDN,CHANNEL,CUSTOMER_CONTACT,TT_TYPE,TT_PRIORITY ,AREA,SUB_AREA,STATUS,RETAIL_SHOP,CURRENT_LOCATION   from flare_8.call_reason where tbl_dt = ${yest}" --output-format CSV_HEADER | sed 's/[\t]/,/g' | awk '{gsub(/\"/,"")};1' >  /mnt/beegfs/ctma/NOKIACSI_TT_${yest}.csv

