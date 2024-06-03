#!/bin/bash

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
                kv="\${MSSDN}=${msisdn}"
                file_name=".${DATE}_CB_SERV_MAST_VIEW_${msisdn}.gz"
                log_file=${DATE}_CB_SERV_MAST_VIEW_${msisdn}.log
                command="bash ${WORK_DIR}/JDBCExtract.scala --configFile ${WORK_DIR}/CB_SERV_MAST_VIEW_config.properties --sqlFile ${WORK_DIR}/CB_SERV_MAST_VIEW_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
        done
wait
