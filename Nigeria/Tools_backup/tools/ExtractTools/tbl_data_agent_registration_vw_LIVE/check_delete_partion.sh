#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

yyyymmdd=`date  "+%Y%m%d"`
yest=$(date -d "-1 day" '+%Y%m%d')
DIRECTORY="hdfs://ngdaas/FlareData/output_8/tbl_data_agent_registration_vw/tbl_dt=${yyyymmdd}/"
full_path="${DIRECTORY}*"
DIRECTORY_yest="hdfs://ngdaas/FlareData/output_8/tbl_data_agent_registration_vw/tbl_dt=${yest}/"
full_path_yest="${DIRECTORY_yest}*"
kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
for i in {1..120}; do
isEmpty=$(hadoop fs -count  hdfs://ngdaas/FlareData/output_8/tbl_data_agent_registration_vw/tbl_dt=${yyyymmdd}/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "Given Path is empty"
    #Do some operation
else
    echo "Given Path is not empty"
                                      hadoop fs -test -d $DIRECTORY_yest
                                                if [ $? == 0 ]
                                                                then
                                                                        echo "${now} Start Delete Old Data for date yest"
                                                                        hadoop fs -rm $full_path_yest
                                                                        echo $full_path_yest
                                                                        echo "${now} End Delete Old Data for date yest"

                                                fi

#                 fi

#    fi
fi

sleep 30
done

