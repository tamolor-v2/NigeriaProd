#!/bin/bash
day=$1
yesterday=`date  "+%Y%m%d" -d "1 day ago"`
cd /mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d"`
date_key=`date  "+%Y-%m-%d"`
echo "$yyyymmdd"
#kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
dt="$(date +"%H%M%S")"
processDay=`date  "+%Y%m%d" -d "${day} day ago"`
 dt=$(date +"%H%M%S")
filename="WBS_PM_RATED_LIVE_${processDay}_${yyyymmdd}_${dt}_${day}.csv"
maxSeq=$( tail -n 1 /mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${day}.txt)
echo "MaxSeq=$maxSeq"
echo "processDay=$processDay"
#$(</mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${msisdnLastDigit}.txt)
echo "started spooling $filename"
echo "$filename"
echo "maxSeq=$maxSeq"
echo "select /*+ parallel 12 */ * from wbs_client.wbs_cdr_${processDay} where process_date > to_timestamp('${maxSeq}','yyyymmddhh24missFF') and ANUM is not null"
/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_CDR/thispwd#456@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.232.166)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ictwbs_p1)))'
set term off
set termout off
set echo off
set underline off
set colsep ','
set pages 40000
SET LONG 50000;
set trimout on
set trimspool on
set feedback off
set heading off
set headsep off
SET LINESIZE 30000
set LONGCHUNKSIZE 30000
set pagesize 0
set wrap off

spool $filename
select  /*+ parallel 6 */ * from wbs_client.wbs_cdr_${processDay} where process_date > to_timestamp('${maxSeq}','yyyymmddhh24missFF') and ANUM is not null;
spool off
quit
EOF
#Removing spaces from the spool file
sed -i '/^[[:space:]]*$/d' $filename
exit
maxSeq=$(cat $filename | awk -F"|" '{print $53}' | sort -nk1 | tail -1) 
if [[ $maxSeq == *"ERROR"* ]]; then
  echo "Error: maxSeq couldn't be set"
  echo "Max_Seq=$maxSeq"
  exit
fi
echo "Max_Seq=$maxSeq"
if [ -z "$maxSeq" ]
	then
	echo "couldn't find new records"
	rm $filename
else
	echo $maxSeq >>/mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${processDate}.txt
	 #dt="$(date +"%H%M%S")"
	echo "time=$dt"
	sort -t"|" -k53 -o $filename $filename
	#awk  -v date=${dt} -F"|" '{print > "/mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/WBS_PM_RATED_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
	#files=(/mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/spool/WBS_PM_RATED_LIVE*)
	tbl_dt=$processDay
	gzip -f $filename
DIRECTORY="hdfs://ngdaas/FlareData/output_8/WBS_PM_RATED_CDRS_LIVE/tbl_dt=${tbl_dt}"
full_path="${DIRECTORY}/*"

hadoop fs -test -d $DIRECTORY
    if [ $? == 0 ]
            then
                echo "Path for date: ${tbl_dt} exists"
	    else
		hadoop fs -mkdir -p /FlareData/output_8/WBS_PM_RATED_CDRS_LIVE/tbl_dt=${tbl_dt}/
                hive -e "msck repair table flare_8.WBS_PM_RATED_CDRS_LIVE"	
	    fi
                hadoop fs -put "${filename}.gz" /FlareData/output_8/WBS_PM_RATED_CDRS_LIVE/tbl_dt=${tbl_dt}
                mkdir -p /mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/old/${tbl_dt}
                mv "${filename}.gz" /mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/old/${tbl_dt}
fi
echo "Max_Seq=$maxSeq"
echo $maxSeq >>/mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/maxSeq_${Day}.txt
#awk -v date="$(date +"%Y%m%d%H%M%S")" -F"|" '{print > "/mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_LIVE/staging/WBS_PM_RATED_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
#gzip -f $filename >$filename$(date +"%Y-%m-%d_%H-%M-%S")

echo "Job: Extract WBS_PM_RATED_LIVE. Status: Finished. Time: $(date +"%Y-%m-%d %H:%M:%S")"
