#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
#MSISDN_FILTER=($4)
DELIMITER='|'
cd ${WORK_DIR}
#        for msisdn in "${MSISDN_FILTER[@]}"; do
                echo "work on DATE: ${DATE}"
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                kv="\${DATE}=${DATE}"
                file_name=".${DATE}_EDW_SIM_SWAP_000.gz"
                log_file=${DATE}_EDW_SIM_SWAP.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/EDW_SIM_SWAP_config.properties --sqlFile ${WORK_DIR}/EDW_SIM_SWAP_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
 #       done
wait
