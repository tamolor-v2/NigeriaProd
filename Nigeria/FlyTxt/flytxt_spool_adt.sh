#!/bin/bash

stream=$1
rundate=$2
lowerstream=`echo $1 | tr A-Z a-z`

spooldir=/ftpout/DaaS_Flytxt_Data/Extracts
logdir=/nas/share05/scripts/log
sqldir=/nas/share05/sql/templates/flytxt
sqlexecdir=/nas/share05/sql/execute

spoolfiletmp=$spooldir/$stream/$stream"_"$rundate".tmp"
spoolfile=$spooldir/$stream/$stream"_"$rundate".csv"
logfile=$logdir/$lowerstream".out"
sqltemplatedropfile=$sqldir/flytxt_drop_$lowerstream".sql"
sqltemplatecreatefile=$sqldir/flytxt_create_$lowerstream".sql"
sqltemplatespoolfile=$sqldir/flytxt_spool_$lowerstream".sql"
sqldropfile=$sqlexecdir/flytxt_drop_$lowerstream".sql"
sqlcreatefile=$sqlexecdir/flytxt_create_$lowerstream".sql"
sqlspoolfile=$sqlexecdir/flytxt_spool_$lowerstream".sql"

#Drop
cp $sqltemplatedropfile $sqldropfile
/usr/bin/presto --server 10.1.197.146:9999 --catalog hive5 --schema nigeria --output-format TSV_HEADER -f $sqldropfile &>> $logfile
#hive -S -f $sqldropfile

#Create
cp $sqltemplatecreatefile $sqlcreatefile
sed -i "s/yyyymmdd/$rundate/g" $sqlcreatefile
/usr/bin/presto --server 10.1.197.146:9999 --catalog hive5 --schema nigeria --output-format TSV_HEADER -f $sqlcreatefile &>> $logfile 

#Spool
case $stream in
RBT_EVENT|VAS_EVENT|PACK_SUB_EVNT|REFILL_EVENT|USG_REALTIME|USG_DATA_SMS|USG_VOICE_OG|USG_VOICE_IC|REVENUE|PROFILE_WEEK|PROFILE_ADAIL|PROFILE_BDAIL)
cp $sqltemplatespoolfile $sqlspoolfile
/usr/bin/presto --server 10.1.197.146:9999 --catalog hive5 --schema nigeria --output-format TSV_HEADER -f $sqlspoolfile > $spoolfiletmp 
sed -i 's/\t/,/g' $spoolfiletmp 
#cp /home/hjanse/spool/flytxt/$stream"_"$rundate".csv" /mnt/beegfs/ftpin/flytxt
#chmod 777 /mnt/beegfs/ftpin/flytxt/$stream"_"$rundate".csv"
mv -f $spoolfiletmp $spoolfile
#gzip -f $spooldir/$stream/$stream"_"$rundate".csv"
chmod 777 $spoolfile
;;
*)
echo "Input Stream not identified: "$stream
;;
esac

exit

