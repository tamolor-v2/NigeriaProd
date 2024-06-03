#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
DELIMITER='|'
cd ${WORK_DIR}
                output_dir=${OUTPUT}/${DATE}
                 log_dir=${WORK_DIR}/logs/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                file_name=".${DATE}_LOCATION_MNP.gz"
                kv="\${FilterDATE}=${DATE}"
                log_file=${DATE}_MTN_LOCATION_REGION.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/MTN_LOCATION_REGION_config.properties --sqlFile ${WORK_DIR}/MTN_LOCATION_REGION_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER}  --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
       
wait
