#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$4
WEEK=$3
DELIMITER='|'
cd ${WORK_DIR}
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                kv="\${date}=${DATE};\${WEEK}=${WEEK}"
                file_name=".${DATE}_MNP_PORTING_BROADCAST.gz"
                log_file=${DATE}_MNP_PORTING_BROADCAST.log
                command="bash ${WORK_DIR}/JDBCExtract.scala --configFile ${WORK_DIR}/MNP_PORTING_BROADCAST_config.properties --sqlFile ${WORK_DIR}/MNP_PORTING_BROADCAST_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
wait
