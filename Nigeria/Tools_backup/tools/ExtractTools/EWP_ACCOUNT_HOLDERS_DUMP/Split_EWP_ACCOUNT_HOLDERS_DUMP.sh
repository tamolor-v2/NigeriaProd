#!/bin/bash
TOOLS_LOCATION=/mnt/beegfs_bsl/Deployment/DEV/Tools
EXT_DEPENDENCY_LIBS=/home/daasuser/FlareCluster/Flare/lib/system
HADOOP_CONFIG=/usr/hdp/current/hadoop-client/conf
USER_ID=daasuser@MTN.COM
KEY_TAP=/etc/security/keytabs/daasuser.keytab
LOG_CONFIG_FILE=${TOOLS_LOCATION}/config/EWP_ACCOUNT_HOLDERS_DUMP_Log4j2.xml
Landing_dir=                #/mnt/beegfs_bsl/live/EWP_ACCOUNT_HOLDERS_DUMP/incoming    #where we recive files from customer
WORKING_DIR=                #/mnt/beegfs_bsl/live/EWP_ACCOUNT_HOLDERS_DUMP/work         #where fileUtiles will do the work
KAMANJA_INCMOING_DIR=       #/mnt/beegfs_bsl/live/EWP_ACCOUNT_HOLDERS_DUMP/split          #where kamanja is consuming files
ARCHIVED_DIR=/mnt/beegfs_bsl/production/archived/EWP_ACCOUNT_HOLDERS_DUMP_unsplitted
DATE_TIME=`date '+%Y%m%d%H%M%S'`
SIZE=4194304 


Usage(){
echo "Usage: [ --landing_dir ] [ --working_dir  ] [ --kamanja_incoming_dir ]"
}


while [ $# -gt 1 ]

do
        case "$1" in
                --landing_dir)
                        Landing_dir=$2
                shift
                ;;
                --working_dir)
                        WORKING_DIR=$2
                shift
                ;;
                --kamanja_incoming_dir)
                        KAMANJA_INCMOING_DIR=$2
                shift
                ;;
        esac
                shift
done


if  [ ! -d "$Landing_dir" ]
then
        echo "Landing_dir is not valid dir"
        Usage
        exit -1
fi

if  [ ! -d "$WORKING_DIR" ]
then
        echo "WORKING_DIR is not valid dir"
        Usage
        exit -1
fi

if  [ ! -d "$KAMANJA_INCMOING_DIR" ]
then
        echo "KAMANJA_INCMOING_DIR is not valid dir"
        Usage
        exit -1
fi

if [ "$(ls -A ${Landing_dir})" ]; then
        echo "${Landing_dir} is not Empty"
else
        echo "${Landing_dir} is Empty.."
        exit -1
fi

current_working_Dir=${WORKING_DIR}/${DATE_TIME}
echo "creating new folder : [ ${current_working_Dir} ]"
mkdir ${current_working_Dir}

original_files_folder=${current_working_Dir}/org_files
echo "creating new folder : [ ${original_files_folder} ]"
mkdir ${original_files_folder}


splitted_files_folder=${current_working_Dir}/splitted_files
echo "creating new folder: [ ${splitted_files_folder} ]"
mkdir ${splitted_files_folder}

echo "moving files from [ ${Landing_dir}/ ] to  [ ${original_files_folder}/] "

mv ${Landing_dir}/*  ${original_files_folder}/

echo "start spliting files.."



command="java -Dlog4j.configurationFile=file:${LOG_CONFIG_FILE}  -cp ${HADOOP_CONFIG}:${EXT_DEPENDENCY_LIBS}/ExtDependencyLibs2_2.11-1.5.3.jar:${EXT_DEPENDENCY_LIBS}/KamanjaInternalDeps_2.11-1.5.3.jar:${TOOLS_LOCATION}/filesUtils.jar com.ligadata.fileutilis.Main --targetType local --sourceType local --maxFileSize ${SIZE} --target ${splitted_files_folder}/ --source  ${original_files_folder}/ --fileExtension .csv.gz " 

echo "executing command : ${command}"

${command}



echo "moving files from [ ${splitted_files_folder}/* ] to [ ${KAMANJA_INCMOING_DIR}/ ]"

mv ${splitted_files_folder}/* ${KAMANJA_INCMOING_DIR}/


echo "moving unsplited file from [ ${original_files_folder}/ ] to [ ${ARCHIVED_DIR} ]"

mv ${original_files_folder}/* ${ARCHIVED_DIR}/

