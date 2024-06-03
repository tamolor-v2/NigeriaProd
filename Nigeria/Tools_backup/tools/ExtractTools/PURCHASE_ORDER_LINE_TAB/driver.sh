#!/bin/bash

WORK_DIR=$1
OUTPUT=$2
DATE=$3 
#HOURS=($4)
#A_NUM_FILTER=($5)
DELIMITER='|'
cd ${WORK_DIR}
#for hour in "${HOURS[@]}"; do
#        echo "work on hour: ${hour}"
#        for anum in "${A_NUM_FILTER[@]}"; do
#                echo "work on anum: ${anum}"
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}


#                kv="\${date}=${DATE};\${hour}=${hour};\${minS}=00;\${minE}=29;\${anum}=${anum}"
                file_name=".${DATE}_PURCHASE_ORDER_LINE_TAB.gz"
                log_file=${DATE}_PURCHASE_ORDER_LINE_TAB.log
                kv="\${DATE}=${DATE}"
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

                echo "starting command: ${command}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

#                kv="\${date}=${DATE};\${hour}=${hour};\${minS}=30;\${minE}=59;\${anum}=${anum}"
#                file_name=".${DATE}_CB_RETAIL_OUTLETS_${hour}_30_0${anum}.gz"
#                log_file=${DATE}_CB_RETAIL_OUTLETS_${hour}_30_0${anum}.log

#                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

#                echo "starting command: ${command}"
#                nohup ${command} >  ${log_dir}/${log_file}  2>&1 &
#        done
wait
#done
