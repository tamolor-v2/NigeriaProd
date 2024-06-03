#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
MSISDN_FILTER=($4)
DELIMITER='|'
cd ${WORK_DIR}
        for msisdn_v in "${MSISDN_FILTER[@]}"; do
                echo "work on msisdn_v: ${msisdn_v}"
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}/${msisdn_v:0:1}/${msisdn_v}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                kv="\${msisdn_v}=${msisdn_v};\${DATE}=${DATE}"
                file_name=".${DATE}_NEWREG_BIOUPDT_POOL_${msisdn_v:0:1}_${msisdn_v}.gz"
                log_file=${DATE}_NEWREG_BIOUPDT_POOL_${msisdn_v:0:1}_${msisdn_v}.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/NEWREG_BIOUPDT_POOL_config.properties --sqlFile ${WORK_DIR}/NEWREG_BIOUPDT_POOL_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
        done
wait
