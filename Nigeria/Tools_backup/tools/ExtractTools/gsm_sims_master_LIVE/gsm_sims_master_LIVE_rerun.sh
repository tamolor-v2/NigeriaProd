yyyymmdd=$1
#Firstday=$(date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m%d)
cd /nas/share05/tools/ExtractTools/gsm_sims_master_LIVE/
bash /nas/share05/tools/ExtractTools/gsm_sims_master_LIVE/gsm_sims_master_LIVE.sh
cd /mnt/beegfs_bsl/tools/ExtractTools/gsm_sims_master_LIVE/tmp/${yyyymmdd}/
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
DIRECTORY="hdfs://ngdaas/FlareData/output_8/gsm_sims_master_LIVE/tbl_dt=${yyyymmdd}/"
full_path="${DIRECTORY}*"
hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "${now} Start Delete Old Data for date ${yyyymmdd}"
                hadoop fs -rm $full_path
                echo $full_path
                echo "${now} End Delete Old Data for date ${yyyymmdd}"

    fi
hadoop fs -mkdir -p /FlareData/output_8/gsm_sims_master_LIVE/tbl_dt=${yyyymmdd}
echo "hadoop fs -mkdir -p /FlareData/output_8/gsm_sims_master_LIVE/tbl_dt=${yyyymmdd}"
#for filename in "/mnt/beegfs_bsl/tools/ExtractTools/gsm_sims_master_LIVE/tmp/20181219/*"; do
for file in *; do
#  echo ${file}
#done

    hadoop fs -put "$file" /FlareData/output_8/gsm_sims_master_LIVE/tbl_dt=${yyyymmdd}
	echo "hadoop fs -put $file /FlareData/output_8/gsm_sims_master_LIVE/tbl_dt=${yyyymmdd}"
done

hive -e "msck repair table flare_8.gsm_sims_master;"
