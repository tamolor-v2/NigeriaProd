#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
second=$4
dt=$5
DELIMITER='|'
cd ${WORK_DIR}
    
                log_dir=${WORK_DIR}/logs/${dt}
                output_dir=${OUTPUT}/${dt}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                kv="\${DATE}=${DATE};\${second}=${second}"
                file_name=".${DATE}_CUG_ACCESS_FEES_part-00000.gz"
                log_file=${DATE}_CUG_ACCESS_FEES_00.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/CUG_ACCESS_FEES_config.properties --sqlFile ${WORK_DIR}/CUG_ACCESS_FEES_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER}  --kvTemplate ${kv} "
                echo "starting command: ${command}"
                echo "log file: ${log_dir}/${log_file}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
      
wait
