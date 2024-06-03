#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
MSISDN_FILTER=($4)
DELIMITER='|'
cd ${WORK_DIR}
        for msisdn in "${MSISDN_FILTER[@]}"; do
                echo "work on msisdn: ${msisdn}"
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}/${msisdn}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                kv="\${msisdn}=${msisdn}"
                file_name=".${DATE}_CALL_REASON_${msisdn}.gz"
                log_file=${DATE}_CALL_REASON_${msisdn}.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/CALL_REASON_config.properties --sqlFile ${WORK_DIR}/CALL_REASON_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
        done
wait
