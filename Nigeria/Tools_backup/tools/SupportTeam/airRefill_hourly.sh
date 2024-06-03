

filename=/mnt/beegfs/tools/SupportTeam/data/air_refill_$(date +"%Y%m%d%H").csv

#datanode
/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "SELECT concat ('234',accountnumber) as  MSISDN,original_timestamp_enrich as Transaction_Date, dedicatedaccountid,try_cast(case when length(accountbalance)=0 then 0.0 else try_cast(accountbalance as double) end as bigint) DA_137_bal_before_recharge_, try_cast(case when length(accountbalance____1 )=0 then 0.0 else try_cast(accountbalance____1  as double) end as bigint) DA_137_bal_after_recharge, try_cast(case when length(refilldivisionamount____1 )=0 then 0.0 else try_cast(refilldivisionamount____1  as double) end as bigint) Recharge_Amount from flare_8.CS5_AIR_REFILL_DA  where tbl_dt=$(date +"%Y%m%d") and dedicatedaccountid=137  and length(accountbalance)=0;"  --output-format TSV_HEADER > $filename 

#edgenode
#/usr/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --execute "SELECT concat ('234',accountnumber) as  MSISDN,original_timestamp_enrich as Transaction_Date, dedicatedaccountid,try_cast(case when length(accountbalance)=0 then 0.0 else try_cast(accountbalance as double) end as bigint) DA_137_bal_before_recharge_, try_cast(case when length(accountbalance____1 )=0 then 0.0 else try_cast(accountbalance____1  as double) end as bigint) DA_137_bal_after_recharge, try_cast(case when length(refilldivisionamount____1 )=0 then 0.0 else try_cast(refilldivisionamount____1  as double) end as bigint) Recharge_Amount from flare_8.CS5_AIR_REFILL_DA  where tbl_dt=$(date +"%Y%m%d")  and length(accountbalance)=0;"  --output-format TSV_HEADER > $filename

#change tabs to comma delimited
sed -i 's/\t/,/g' $filename
chmod 777 $filename


bash  /mnt/beegfs/tools/SupportTeam/data/scripts/flytxt_job.sh
