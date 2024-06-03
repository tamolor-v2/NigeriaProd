quarter=$(date -d '-5 day' '+%Y%m%d')
for number in 0 1 2 3 4 5 6 7 8 9 
do

/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute " select msisdn_key ,segment from nigeria.segment5b5 where aggr='quarterly' and cast (msisdn_key as varchar) like '%${number}' and tbl_dt= ${quarter} limit 1000000
 " --output-format CSV_HEADER | sed 's/[\t]/,/g' | awk '{gsub(/\"/,"")};1' >    /home/rqader/CustomerSegmentation${number}.csv
done

