#!/bin/bash

WORK_DIR=$1
OUTPUT=$2
DATE=$3
DELIMITER='|'
cd ${WORK_DIR}
                output_dir=${OUTPUT}/${DATE}
                 log_dir=${WORK_DIR}/logs/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                file_name=".${DATE}_AGL_BILL_ANALYZER_MONTHLY_part-00000.gz"
                kv="\${DATE}=${DATE}"
                log_file=${DATE}_AGL_BILL_ANALYZER_MONTHLY.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/AGL_BILL_ANALYZER_MONTHLY_config.properties --sqlFile ${WORK_DIR}/AGL_BILL_ANALYZER_MONTHLY_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER}  --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
       
wait
