#!bin/bash

DATE=$(date -d '-1 day' '+%Y%m%d')
DATE_V=$(date +"%Y%m%d %H:%m:%S")
echo "Start running for ${DATE} at ${DATE_V}"

 /opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "insert into flare_8.share_and_sell_summary select  msisdn,beneficiary_msisdn,f3pp_transactionid,correlation_id,channel_name,product_id,product_name,product_type,product_subtype,charging_amount,case when  upper(status) in ('SUCCESS') then  charging_amount else 'NA'   end , transaction_charges, status, ma.service_class_id,cis.tbl_dt from flare_8.cis_cdr cis left outer join nigeria.daily_sdp_dump_ma ma on (ma.tbl_dt = cis.tbl_dt and ma.msisdn_key = cis.msisdn_key) where ( cis.product_type like '%ShareNSell%' or upper(trim(cis.product_type)) like  ('MTN%SHARE') ) and  cis.tbl_dt  = ${DATE}"
 q1= /opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "select count(*) from flare_8.share_and_sell_summary where tbl_dt=${DATE}"
 q2= /opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "select count(cis.msisdn_key) from flare_8.cis_cdr cis left outer join nigeria.daily_sdp_dump_ma ma on (ma.tbl_dt = cis.tbl_dt and ma.msisdn_key = cis.msisdn_key) where ( cis.product_type like '%ShareNSell%' or upper(trim(cis.product_type)) like  ('MTN%SHARE') ) and  cis.tbl_dt  = ${DATE}"
 q11=$(echo $q1 |  sed "s/\"//g")
 q22=$(echo $q2 |  sed "s/\"//g")
 q=$((q11-q22))
 res=$(echo $q)
 if [ $res == 0 ]
 then
  echo "Finished for : ${DATE} successfully" 
 else
  echo "Failed to insert the date to the table due to Presto issue or mismatch counts, Please rerun me! "
 fi
