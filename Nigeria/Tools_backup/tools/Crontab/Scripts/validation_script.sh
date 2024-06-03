#!/bin/bash 

###yest=$(date -d '-1 day' '+%Y%m%d')
yest=$1
PIDFILE=/home/daasuser/PIDFiles/validation.pid
check_pid_file(){
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Job is already running"
#    echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived already running for $yest>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
 #   exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
  #    echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived for $yest Could not create PID file>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
      echo "Could not create PID file"
   #   exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
   # echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived for $yest Could not create PID file>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
    echo "Could not create PID file"
    #exit 1
  fi
fi
}
removeDir=$(date -d '-60 day' '+%Y%m%d')
mytime() {
date +"%Y-%m-%d_%H-%M-%S"
}

main_process(){


#Send Start Validation Email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Start Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool Started for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com,  wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"

bash /home/daasuser/ValidationTool_Load/bin/InvokeValidation_new.sh $yest all VRDSF  2>&1 | tee /home/daasuser/ValidationTool_Load/logs/ValidationTool_$(date +%Y%m%d_%s).txt


#Send Finish validation Email
echo -e "CronJob \"line_count_archived_withValidation.sh\" Finished Validation at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Note_NG@edge01001.mtn.com" -s "DAAS_Note_MTN_NG_<Validation_Tool Finished for $yest at $(date +"%Y-%m-%d %H:%M:%S")>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"


rm $PIDFILE
}

for ((i=1; i<=3; i++))
do
check_pid_file
retval=$?
if [ "$retval" == 2 ]
then
     echo "attempt $i, sleep 2m"
     echo -e "CronJob \"line_count_archived_withValidation.sh\" Started LineCount for archived files at $(date +"%T") on edge01002, for day: $yest \n" | mailx -r "DAAS_Alert_NG@edge01001.mtn.com" -s "DAAS_Alert_MTN_NG_<Line_Count_Archived already running for $yest, attempt $i>" "yulbeh@ligadata.com, wsbayee@ligadata.com, samer@ligadata.com, hendre@ligadata.com, timipa@ligadata.com, krishna@ligadata.com, saleh@ligadata.com"
     sleep 20m
else
     main_process
     break
fi
done
