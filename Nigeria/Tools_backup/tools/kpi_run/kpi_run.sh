#!/bin/bash
cd /mnt/beegfs/tools/kpi_run

rundate=$1
enddate=$2

#Loop through the requested run days
while [ $rundate -le $enddate ]
do
echo $rundate
bash ./kpi_run_val_email.sh $rundate
rundate=$( date -d"$rundate +1 day" +%Y%m%d )
done
exit
