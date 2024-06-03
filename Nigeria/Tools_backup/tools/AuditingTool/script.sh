#!/bin/sh

# General params
###############
#DATE
dt=$(date +"%Y%m%d")

#presto connection
prestoParam="/opt/presto/bin/presto --server master01003:8999 --catalog hive5"

#dim table name
dimTable="dim_job"

#split char -must be fix-
splitChar=$(printf '\001')

#Job start time
startTime=$(date +"%Y-%m-%d %H:%M:%S.%3N")

#must be passed (check dim table for jobs IDs)
id=$1
##################
errorMsg=""
#Checking the params (the optional and the mondatory ones)
if [[ $# -eq 3 ]]
then
#PCF,VR,Encryption,SFTP etc.. cmd
jobCmd=$3
detailedMsg=$2
errorMsg=$( $jobCmd 2>&1)
elif [[ $# -eq 2 ]]
then
#PCF,VR,Encryption,SFTP etc.. cmd
jobCmd=$2
detailedMsg=""
errorMsg=$( $jobCmd 2>&1)
elif [[ $# -eq 0 ]]
then
echo -e "ERROR: You must pass the Job ID!\nINFO: Check the dim.dim_job table for a valid job ID.."
exit 1;
else
echo "ERROR: You must pass the job ID AND the cmd you want to run at the correct format!"
exit 1;
fi

#stats check
res=$?
if [[ $res == 0 ]]
then
stats='Success' 
else
stats='Failed'
echo $errorMsg
detailedMsg=$errorMsg
fi

#Job end time
endTime=$(date +"%Y-%m-%d %H:%M:%S.%3N")

# getting the report name from the dim table
reportNameTmp=$($prestoParam --schema dim --execute "select jobname from $dimTable where jobid = '${id}' ")
if [[ $reportNameTmp != '' ]]
then 
reportName=$(echo "$reportNameTmp" | tr -d '"')
else
echo -e "ERROR: This job ID $id is not at the $dimTable table!\nINFO: Check the dim.$dimTable table for a valid job ID.."
exit 1;
fi

# NG = yes , UG,GC = NO
kinit="yes"

msg="${id}${splitChar}${reportName}${splitChar}${startTime}${splitChar}${endTime}${splitChar}${stats}${splitChar}${detailedMsg}${splitChar}${dt}"
echo "$msg"
 if [ $kinit == "yes" ]  
 then
 # kinit -kt /etc/security/keytabs/kafka.service.keytab kafka/datanode01038.mtn.com@MTN.COM
  java -Dsun.security.krb5.debug=true -Djava.security.auth.login.config=/etc/kafka/2.6.1.0-129/0/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -DjavasplitChar.security.auth.useSubjectCredsOnly=false  -cp /nas/share05/tools/AuditingTool/lib/AlertProducer-assembly-0.1.jar AlertProducer.AlertProducer --configFile /nas/share05/tools/AuditingTool/conf/AlertProducer.json --message "$msg"
 else
  java -Dsun.security.krb5.debug=true -Djava.security.auth.login.config=/etc/kafka/2.6.1.0-129/0/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -DjavasplitChar.security.auth.useSubjectCredsOnly=false  -cp /nas/share05/tools/AuditingTool/lib/AlertProducer-assembly-0.1_withoutK.jar AlertProducer.AlertProducer --configFile /nas/share05/tools/AuditingTool/conf/AlertProducer.json --message "$msg"
 fi  
