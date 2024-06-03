#!/bin/bash

WORK_DIR=$1
OUTPUT=$2
DATE=$3
DELIMITER='|'
cd ${WORK_DIR}
                output_dir=${OUTPUT}/${DATE}
                 log_dir=${WORK_DIR}/logs
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                file_name=".${DATE}_SIMREG_AGL_POOL_RPT.gz"
                kv="\${DATE}=${DATE}"
                log_file=${DATE}_SIMREG_AGL_POOL_RPT.log
                command="bash ${WORK_DIR}/JDBCExtract.scala --configFile ${WORK_DIR}/SIMREG_AGL_POOL_RPT_config.properties --sqlFile ${WORK_DIR}/SIMREG_AGL_POOL_RPT_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER}  --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
       
wait
