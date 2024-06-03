#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

#PIDFILE=/home/daasuser/PIDFiles/extractIncremental_COMPENSATION_LIVE.pid


yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename=$yest"_"$yyyymmdd"_COMPENSATION_LIVE"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
DIRECTORY="hdfs://ngdaas/FlareData/output_8/COMPENSATION/tbl_dt=${yyyymmdd}/"
full_path="${DIRECTORY}*"
DIRECTORY_yest="hdfs://ngdaas/user/mtn_user/kamanja_test/COMPENSATION/"
#full_path_yest="${DIRECTORY_yest}*"
full_path_file="${DIRECTORY}${filename}.gz"
startdate=$(date -d '-41 day' '+%Y%m%d')
enddate=$(date -d '-1 day' '+%Y%m%d')
#startdate=20141030
#enddate=20141120
dates=()
for (( date="$startdate"; date != enddate; )); do
    dates+=( "$date" )
    date="$(date --date="$date + 1 days" +'%Y%m%d')"
done

#echo "${dates[@]}"
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
bash /mnt/beegfs_bsl/tools/ExtractTools/COMPENSATION/COMPENSATION.sh $yest
#retVal=$?
#if [ $retVal -eq 0 ];
#then
for Item in ${dates[*]}
    do
full_path_yest="${DIRECTORY_yest}tbl_dt=$Item/*"
hadoop fs -test -d $DIRECTORY_yest
if [ $? == 0 ]
then
echo "${now} Start Delete Old Data for date $full_path_yest"
hadoop fs -rm -R $full_path_yest
echo $full_path_yest
echo "${now} End Delete Old Data for date $full_path_yest"

fi
done

echo "move files..."
rmdir /mnt/beegfs_bsl/FlareDataTest/COMPENSATION/incoming/$yest
mv /mnt/beegfs_bsl/tools/ExtractTools/COMPENSATION/tmp/$yest /mnt/beegfs_bsl/FlareDataTest/COMPENSATION/incoming/

#fi




kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hadoop fs -mkdir -p /FlareData/output_8/COMPENSATION/tbl_dt=${yyyymmdd}
hadoop fs -mkdir -p /FlareData/output_8/COMPENSATION/tbl_dt=${yest}
hive -e "msck repair table kamanja_test.COMPENSATION;"

rm $PIDFILE
echo "Job: Extract COMPENSATION_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
exit

