#!/bin/bash

WORK_DIR=$1
OUTPUT=$2
DATE=$3 
nowmonth=$(date +%Y-%m)
prevmont=$(date -d "$nowmonth-15 last month" '+%Y%m')
currmonth=$(date +%Y%m)	
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
                file_name=".${DATE}_ACCOUNTING_BALANCE_TAB.gz"
                log_file=${DATE}_ACCOUNTING_BALANCE_TAB.log
                kv="\${DATE}=${DATE};\${prevmont}=${prevmont};\${currmonth}=${currmonth}"
                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

                echo "starting command: ${command}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

#                kv="\${date}=${DATE};\${hour}=${hour};\${minS}=30;\${minE}=59;\${anum}=${anum}"
#                file_name=".${DATE}_ACCOUNTING_BALANCE_TAB_${hour}_30_0${anum}.gz"
#                log_file=${DATE}_ACCOUNTING_BALANCE_TAB_${hour}_30_0${anum}.log

#                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

#                echo "starting command: ${command}"
#                nohup ${command} >  ${log_dir}/${log_file}  2>&1 &
#        done
wait
#done