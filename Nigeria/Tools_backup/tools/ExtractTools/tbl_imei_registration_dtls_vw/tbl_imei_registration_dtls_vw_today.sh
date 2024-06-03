#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

PIDFILE=/home/daasuser/PIDFiles/extractIncremental_tbl_imei_registration_dtls_vw.pid
check_pid_file(){
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
    #exit 1
return 2
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
return 2
      #exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    #exit 1
return 2
  fi
fi
}


file=/mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/staging/maxSeq.txt
yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
filename=$yest"_"$yyyymmdd"_tbl_imei_registration_dtls_vw"
load_date=`date '+%Y-%m-%d %H:%M:%S'`
DIRECTORY="hdfs://ngdaas/FlareData/output_8/tbl_imei_registration_dtls_vw/tbl_dt=${yyyymmdd}/"
full_path="${DIRECTORY}*"
DIRECTORY_yest="hdfs://ngdaas/FlareData/output_8/tbl_imei_registration_dtls_vw/tbl_dt=${yest}/"
full_path_yest="${DIRECTORY_yest}*"
full_path_file="${DIRECTORY}${filename}.gz"


bash /nas/share05/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh $yest
retVal=$?
#if [ $retVal -eq 0 ];
#then
	   				   hadoop fs -test -d $DIRECTORY_yest
						if [ $? == 0 ]
								then
									echo "${now} Start Delete Old Data for date yest"
									#hadoop fs -rm $full_path_yest
									echo $full_path_yest
									echo "${now} End Delete Old Data for date yest"

						fi
                        rmdir /mnt/beegfs_bsl/live/DB_extract_lz/tbl_imei_registration_dtls_vw/incoming/$yest
                        rmdir /mnt/beegfs_bsl/FlareData/CDR/tbl_imei_registration_dtls_vw/incoming/$yest
                        mv /nas/share05/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/$yest /mnt/beegfs_bsl/live/DB_extract_lz/tbl_imei_registration_dtls_vw/incoming/

#fi

bash /nas/share05/tools/ExtractTools/tbl_imei_registration_dtls_vw/tbl_imei_registration_dtls_vw.sh $yyyymmdd
retVal=$?
#if [ $retVal -eq 0 ];
#then
	   				   hadoop fs -test -d $DIRECTORY
						if [ $? == 0 ]
								then
									echo "${now} Start Delete Old Data for date yest"
									#hadoop fs -rm $full_path
									echo $full_path
									echo "${now} End Delete Old Data for date yest"

						fi
                        rmdir /mnt/beegfs_bsl/live/DB_extract_lz/tbl_imei_registration_dtls_vw/incoming/$yyyymmdd
                        rmdir /mnt/beegfs_bsl/FlareData/CDR/tbl_imei_registration_dtls_vw/incoming/$yyyymmdd
                        mv /nas/share05/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/$yyyymmdd  /mnt/beegfs_bsl/live/DB_extract_lz/tbl_imei_registration_dtls_vw/incoming/

#fi
#results=( $(find /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/$yest  -name $yest"_"$yyyymmdd"_tbl_imei_registration_dtls_vw.gz"))
#if (( ${#results[@]} )) ; then
#    if [[ -s /mnt/beegfs_bsl/tools/ExtractTools/tbl_imei_registration_dtls_vw/tmp/$yest/$yest"_"$yyyymmdd"_tbl_imei_registration_dtls_vw.gz ]]; then 
#	   echo "file has   something"; 
#	   				   hadoop fs -test -d $DIRECTORY_yest
#						if [ $? == 0 ]
#								then
#									echo "${now} Start Delete Old Data for date yest"
#									hadoop fs -rm $full_path_yest
#									echo $full_path_yest
#									echo "${now} End Delete Old Data for date yest"
#
#						fi
					
#	else 
#	   echo "file is empty"; fi
#else
#   echo Not found
#fi


kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
hadoop fs -mkdir -p /FlareData/output_8/tbl_imei_registration_dtls_vw/tbl_dt=${yyyymmdd}
hadoop fs -mkdir -p /FlareData/output_8/tbl_imei_registration_dtls_vw/tbl_dt=${yest}

hive -e "msck repair table flare_8.tbl_imei_registration_dtls_vw;"

rm $PIDFILE
echo "Job: Extract tbl_imei_registration_dtls_vw. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")" 
exit
