cd /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/spool
export ORACLE_BASE=/usr/lib/oracle
export ORACLE_HOME=/usr/lib/oracle/product/11.1.0/client_1
#BIB_CTL/h872sgf#kk@'(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.1.218.168)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SID=BIODSP12)))'
#start_date=$1
#end_date=$2
yyyymmdd=`date  "+%Y%m%d"`
echo "$yyyymmdd"
filename="NEWREG_BIOUPDT_POOL_LIVE_"$yyyymmdd".csv"

sed -i '/^[[:space:]]*$/d' $filename
maxSeq=$(cat $filename | awk -F"|" '{print $1}' | sort -nk1 | tail -1)
 dt="$(date +"%H%M%S")"
echo "time=$dt"
sort -t"|" -k14 -o $filename $filename
awk  -v date=${dt} -F"|" '{print > "/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/NEWREG_BIOUPDT_POOL_LIVE_"substr($14,1,8)"_"date".txt"}' $filename
files=(/mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/staging/NEWREG_BIOUPDT_POOL_LIVE*)
for var in "${files[@]}"
do
echo "f_n: ${var}"
tbl_dt="${var:89:8}"
re='^[0-9]+$'
if ! [[ $tbl_dt =~ $re ]]
then
   echo "file name : $var, tbl_dt couldn't be extracted, extracted date = $tbl_dt"
else
echo "var=$var"
gzip -f $var
tbl_dt="${var:83:8}"
#hadoop fs -mkdir -p /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}
echo "hadoop fs -mkdir -p /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}"
echo "hadoop fs -put '${var}.gz' /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}"
#hadoop fs -put "${var}.gz" /FlareData/output_8/NEWREG_BIOUPDT_POOL_LIVE/tbl_dt=${tbl_dt}
#mkdir -p /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/old/${tbl_dt}
#mv "${var}.gz" /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/old/${tbl_dt}/
fi
  # do something on $var
done
#hadoop fs -put -f /home/daasuser/spool/$filename /user/hive/flare/wbs_bib_report/
mv $filename ${tbl_dt}_$dt_$filename
gzip -f ${tbl_dt}_$dt_$filename

#mv $filename "$filename_$(date +"%Y-%m-%d_%H-%M-%S").csv.gz"
echo "Max_Seq=$maxSeq"

