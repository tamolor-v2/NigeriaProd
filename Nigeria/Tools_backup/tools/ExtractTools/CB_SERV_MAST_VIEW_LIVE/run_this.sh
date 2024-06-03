#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


startdate=$1
enddate=$2
stream=$3
app=$4

rundate=$startdate
echo "startdate=$startdate"
echo "enddate=$enddate"
echo "stream=$stream"
echo "app=$app"
logdir=/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/log
sqldir=/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/$app
sqlexecdir=/mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/execute
logfile=$logdir/$stream".out"
sqltemplatefile=$sqldir/$stream".sql"
sqltemplatefile_test=$sqldir/$stream"_test.sql"
sqlfile=$sqlexecdir/$stream".sql"
echo "logdir=$logdir"
echo "sqldir=$sqldir"
echo "sqlexecdir=$sqlexecdir"
echo "logfile=$logfile"
echo "sqltemplatefile=$sqltemplatefile"
echo "sqlfile=$sqlfile"
#Validate if startdate is greater or equal than end date due to the script running backwards from largest to smallest date
if [ $startdate -gt $enddate ]
then
echo "Startdate cannot be greater than enddate. First date parameter cannot be greater than second date parameter." &>> $logfile
exit
fi
echo " ----Run start for "$stream" for period "$startdate" --> "$enddate". "`date +"%Y.%m.%d-%H:%M:%S"`"----" &>>$logfile

while [ $rundate -le $enddate ]
do
echo $rundate >>$logfile

for i in {0..9}
do
#Copy the template and change date


#truncate -s 0 $sqlfile
cp $sqltemplatefile_test $sqlfile
#sed -i "s/yyyymmdd/${rundate}/g" $sqlfile
#sed -i "s/last_digit/${i}/g" $sqlfile 
#sed -i "s/yyyymmdd/$rundate/g" $sqlfile 
sed -i -e "s/yyyymmdd/${rundate}/g" -e "s/last_digit/${i}/g" $sqlfile
#sed -i -e "s/yyyymmdd/${rundate}/g;t;s/last_digit/${i}/g" $sqlfile 
#sed -i -e "s/last_digit/${i}/g" $sqlfile

#sed -i "s/yyyymmdd/${rundate}/g" $sqlfile
#Remove partition date
#hadoop fs -ls -C hdfs://ngdaas/user/hive/nigeria/$stream/tbl_dt=$rundate* | awk '{print "fs -rm -f "$0}' | xargs hadoop &>> $logfile
echo `date +"%Y.%m.%d-%H:%M:%S"` &>>$logfile

#Run presto SQL to insert the partition that was remove above
/opt/presto/bin/presto --server master01004:8099 --catalog hive5 --schema nigeria --output-format CSV_HEADER --debug -f $sqlfile &>> $logfile
echo `date +"%Y.%m.%d-%H:%M:%S"` &>>$logfile
done
rundate=$( date -d"$rundate +1 day" +%Y%m%d )
done

echo " ----Run end for "$stream" for period "$startdate" --> "$enddate". "`date +"%Y.%m.%d-%H:%M:%S"`"----" &>>$logfile

exit

