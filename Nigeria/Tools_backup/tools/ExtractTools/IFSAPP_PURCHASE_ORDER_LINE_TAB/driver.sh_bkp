#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3 
# HOURS=($4)
#A_NUM_FILTER=($5)
DELIMITER='|'
cd ${WORK_DIR}
#for HOUR in "${HOURS[@]}"; do
#        echo "work on hour: ${hour}"
#        for anum in "${A_NUM_FILTER[@]}"; do
#                echo "work on anum: ${anum}"
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}


                kv="\${DATE}=${DATE}"
                file_name="${DATE}_IFSAPP_PURCHASE_ORDER_LINE_TAB.gz"
                log_file=${DATE}_IFSAPP_PURCHASE_ORDER_LINE_TAB.log
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
#        done
wait
#done
