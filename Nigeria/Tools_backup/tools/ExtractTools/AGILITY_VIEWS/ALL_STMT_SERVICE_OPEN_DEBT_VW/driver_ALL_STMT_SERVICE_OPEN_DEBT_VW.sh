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
                file_name=".${DATE}_ALL_STMT_SERVICE_OPEN_DEBT_VW.gz"
                kv="\${FilterDATE}=${DATE}"
                log_file=${DATE}_ALL_STMT_SERVICE_OPEN_DEBT_VW.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/ALL_STMT_SERVICE_OPEN_DEBT_VW_config.properties --sqlFile ${WORK_DIR}/ALL_STMT_SERVICE_OPEN_DEBT_VW_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER}  --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
       
wait
