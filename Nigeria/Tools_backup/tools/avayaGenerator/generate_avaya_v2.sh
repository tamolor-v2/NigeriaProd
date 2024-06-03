#!/bin/sh
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
if [ $# -eq 0 ]
  then
 	echo "No arguments supplied"
        dt=$(date --date="1 day ago" +"%Y%m%d")
else
	dt=$1
fi
echo "$dt"
#hive -S -e "use flare_8; msck repair table avaya_ivr;msck repair table avaya_clid"
 hive -e "use flare_8; msck repair table avaya_ivr;msck repair table avaya_clid;"
IVR_records_count=$(/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format  CSV_HEADER --execute  "select count(*) records_count from flare_8.avaya_ivr where tbl_dt=${dt}")
cln_rec_cnt=$(echo "$IVR_records_count" | sed -e 's/"//g')
arr=($cln_rec_cnt)
rec_cnt[0]=${arr[1]}


CLID_records_count=$(/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format  CSV_HEADER --execute  "select count(*) records_count from flare_8.avaya_clid where tbl_dt=${dt}")
clid_cln_rec_cnt=$(echo "$CLID_records_count" | sed -e 's/"//g')
arr=($clid_cln_rec_cnt)
rec_cnt[1]=${arr[1]}


#$(echo $IVR_records_count | tr " " "\n")
echo "ive_rec_cnt===> ${rec_cnt[0]}"
echo "clid_rec_cnt===> ${rec_cnt[1]}"
files_dir=/mnt/beegfs/tools/avayaGenerator/data/${dt}
if  [[ "${rec_cnt[0]}" != "0" && "${rec_cnt[1]}" != "0" ]]
then
	echo "Greater than 0"
	if [ ! -d "$files_dir" ]
	then
	mkdir ${files_dir}
	
	hive -e "set hive.cli.print.header=true;set tez.am.resource.memory.mb = 10048;set hive.tez.container.size=10000;set tez.runtime.io.sort.mb = 22096;  select date_format(from_unixtime(cast(date_key_customer/1000 as bigint)),'dd-MM-yyyy HH:mm:ss') date_key_customer ,uniqueid,msisdn,language_dsc,connectiontype,segment,starttime,callduration,vdndetails,servicename,accesscode,disconnectreason,endtime,exitlocation,firstname,lastname from flare_8.avaya_ivr  where tbl_dt=$dt" | sed 's/[\t]/,/g'  > ${files_dir}/NokiaCSI_IVR_"$dt".csv
	
	hive -e "set hive.cli.print.header=true;set tez.am.resource.memory.mb = 10048;set hive.tez.container.size=10000;set tez.runtime.io.sort.mb = 22096; select mobile,agentnameid,skillsetname,callland_time,start_time,end_time,handlingtime,finaldisposition  from flare_8.avaya_clid where file_name like '%CLID2%' and tbl_dt=$dt" | sed 's/[\t]/,/g'  > ${files_dir}/NokiaCSI_CLID2_"$dt".csv
	
	hive -e "set hive.cli.print.header=true;set tez.am.resource.memory.mb = 10048;set hive.tez.container.size=10000;set tez.runtime.io.sort.mb = 22096; select mobile,agentnameid,skillsetname,callland_time,start_time,end_time,handlingtime,finaldisposition  from flare_8.avaya_clid where file_name like '%CLID1%' and tbl_dt=$dt" | sed 's/[\t]/,/g'  > ${files_dir}/NokiaCSI_CLID1_"$dt".csv
chmod 777 -R ${files_dir}
scp  ${files_dir}/* 10.1.197.141:/ftpout/DaaS_ctma_data/avaya/
	else
	echo "already generated"
	fi
else
echo "Equal 0"
fi
exit
 hive -e "set hive.cli.print.header=true;set tez.am.resource.memory.mb = 10048;set hive.tez.container.size=10000;set tez.runtime.io.sort.mb = 22096; select date_format(from_unixtime(cast(date_key_customer/1000 as bigint)),'dd-MM-yyyy HH:mm:ss') date_key_customer ,uniqueid,msisdn,language_dsc,connectiontype,segment,starttime,callduration,vdndetails,servicename,accesscode,disconnectreason,endtime,exitlocation,firstname,lastname from flare_8.avaya_ivr  where tbl_dt=$dt" | sed 's/[\t]/,/g'  > NokiaCSI_IVR_"$dt".csv

 hive -e "set hive.cli.print.header=true;set tez.am.resource.memory.mb = 10048;set hive.tez.container.size=10000;set tez.runtime.io.sort.mb = 22096; select mobile,agentnameid,skillsetname,callland_time,start_time,end_time,handlingtime,finaldisposition  from flare_8.avaya_clid where file_name like '%CLID2%' and tbl_dt=$dt" | sed 's/[\t]/,/g'  > NokiaCSI_CLID2_"$dt".csv

 hive -e "set hive.cli.print.header=true;set tez.am.resource.memory.mb = 10048;set hive.tez.container.size=10000;set tez.runtime.io.sort.mb = 22096; select mobile,agentnameid,skillsetname,callland_time,start_time,end_time,handlingtime,finaldisposition  from flare_8.avaya_clid where file_name like '%CLID1%' and tbl_dt=$dt" | sed 's/[\t]/,/g'  > NokiaCSI_CLID1_"$dt".csv
#processing_dt=$(date --date $dt +"%Y%m%d")
#echo $processing_dt

