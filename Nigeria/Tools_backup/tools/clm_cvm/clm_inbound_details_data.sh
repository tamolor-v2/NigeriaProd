#!/bin/bash

INPUTSTARTDATE=$1
if [ -z "$INPUTSTARTDATE" ]; then
echo "Input INPUTSTARTDATE is not provided. Going to run for yesterday"
STARTDATE=$(date -d '-1 day' '+%Y%m%d')
else 
echo "Going to run for date parameter $INPUTSTARTDATE"
STARTDATE=$INPUTSTARTDATE
fi


sqldir=/mnt/beegfs_bsl/scripts/clm_cvm/clm_inbound_details

#validation on undeline tables not empty

sqltemplatevalidation=$sqldir/"input_validation_template.sql"
sqlexecvalidation=$sqldir/exec/"input_validation_exec.sql"
cp $sqltemplatevalidation $sqlexecvalidation
sed -i "s/v_yyyy_mm_dd/$STARTDATE/g" $sqlexecvalidation


commandValid=`/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema nigeria  --debug -f "${sqlexecvalidation}"`
qbase=$(echo $commandValid |  sed "s/\"//g")
echo $qbase
if [ "$qbase" = "T" ];
then
#hadoop fs -rm -r hdfs://ngdaas/user/hive/nigeria/clm_inbound_details_data/*/*

sqltemplatereport=$sqldir/"report_script_template.sql"
sqlexecreport=$sqldir/exec/"report_script_exce.sql"
cp $sqltemplatereport $sqlexecreport
sed -i "s/v_yyyy_mm_dd/$STARTDATE/g" $sqlexecreport
#run report script
/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema nigeria  --debug -f "${sqlexecreport}"
#validate output not 0
q1=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select count(*) from nigeria.clm_inbound_details_data where tbl_dt = ${STARTDATE}")
q11=$(echo $q1 |  sed "s/\"//g")
res=$(echo $q11)
if [[ $res != 0 ]]
then
 echo "Insertion finished for date: $STARTDATE successfully"
else
 echo "Couldn't Insert the data due to Presto issue or mismatching numbers, Please rerun me! "
 exit 1
fi
else
 echo "one base tables is empty"
 exit 1
fi


