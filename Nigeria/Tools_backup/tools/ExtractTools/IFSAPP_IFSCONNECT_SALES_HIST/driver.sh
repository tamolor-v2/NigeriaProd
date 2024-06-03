#!/bin/bash
WORK_DIR=$1
OUTPUT=$2
DATE=$3 
DELIMITER='|'
cd ${WORK_DIR}
log_dir=${WORK_DIR}/logs/${DATE}
output_dir=${OUTPUT}/${DATE}
mkdir -p ${log_dir}
mkdir -p ${output_dir}
echo $DATE

kv="\${DATE}=${DATE};"
file_name="${DATE}_IFSAPP_IFSCONNECT_SALES_HIST.gz"
log_file=${DATE}_IFSAPP_IFSCONNECT_SALES_HIST.log
command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

echo "starting command: ${command}"
nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

wait

# # !/bin/bash
# WORK_DIR=$1
# OUTPUT=$2
# DATE=$3 
# # HOURS=($4)
# #A_NUM_FILTER=($5)
# DELIMITER='|'
# cd ${WORK_DIR}

# day=5
# DATE=$(date -d "-$day day" '+%Y%m%d')
# while [ $DATE -gt 20190101 ]
# do
#     DATE=$(date -d "-$day day" '+%Y%m%d')
#     echo $DATE
#     log_dir=${WORK_DIR}/logs/${DATE}
#     output_dir=${OUTPUT}/${DATE}
#     mkdir -p ${log_dir}
#     mkdir -p ${output_dir}

#     kv="\${DATE}=${DATE};"
#     file_name="${DATE}_IFSAPP_IFSCONNECT_SALES_HIST.gz"
#     log_file=${DATE}_IFSAPP_IFSCONNECT_SALES_HIST.log
#     command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
#     echo "starting command: ${command}"
#     nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &
#     # mv ${output_dir} /mnt/beegfs_bsl/live/DB_extract_lz/IFSAPP_IFSCONNECT_SALES_HIST/incoming/
#     ((day++))

# wait
# done
