yest=$(date -d '-1 day' '+%Y%m%d')
today=$(date +"%Y%m%d")
seq_file="/nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/staging/maxSeq_${today}.dat"
seq=$(tail -2 ${seq_file} | head -1)
emailReceiver=$(cat /nas/share05/tools/Crontab/Scripts/email.dat)
echo $seq

export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1

source_count=($(/usr/lib/oracle/product/11.1.0/client_1/bin/sqlplus -S <<EOF
DAAS_CDR/DAAS_pwd#457@10.1.232.166:1521/ictwbs_p1
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
select  /*+ parallel 8 */ count(*)  from WBS_CLIENT.WBS_CDR_${today} WHERE   process_date < to_date('${seq}','yyyymmddhh24miss') and ANUM is not null;
EOF))

echo $source_count
hive -e "msck repair table flare_8.wbs_pm_rated_cdrs"

hive_count=$(/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema flare_8 --output-format  CSV_HEADER --execute "select count(*) from flare_8.wbs_pm_rated_cdrs where tbl_dt=${today} and date_format(date_parse(process_date,'%Y-%m-%d %H:%i:%s.%f'),'%Y%m%d%H%i%s') <'${seq}' ;")
echo $hive_count
#hive -e "msck repair table flare_8.wbs_pm_rated_cdrs"
ssh edge01002 " echo -e 'ronJob \"check_PM_RATED.sh\"  at $(date +"%T"), <for seq $seq count of source is : $source_count and count of hive is : $hive_count for day: $today \n' | mailx -r 'DAAS_VALIDATE_NG@edge01001.mtn.com' -s 'DAAS_VALIDATE_MTN_NG_< at $(date +"%Y-%m-%d %H:%M:%S")>' '$emailReceiver'"
echo "Job: check_PM_RATED.  Time: $(date +"%Y-%m-%d %H:%M:%S")"
