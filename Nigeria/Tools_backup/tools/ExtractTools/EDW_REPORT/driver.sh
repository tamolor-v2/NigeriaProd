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
tbl_name=$(date --date="$DATE" "+%d_%m_%Y")

kv="\${DATE}=${tbl_name};"
file_name="${DATE}_EDW_REPORT.gz"
log_file=${DATE}_EDW_REPORT.log
command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

echo "starting command: ${command}"
nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

wait

# # #!/bin/bash

# WORK_DIR=$1
# OUTPUT=$2
# DATE=$3 
# # HOURS=($4)
# #A_NUM_FILTER=($5)
# DELIMITER='|'
# cd ${WORK_DIR}

# for i in {07..08}; do
#     DATE=201905$i
#     echo $DATE

#     log_dir=${WORK_DIR}/logs/${DATE}
#     output_dir=${OUTPUT}/${DATE}
#     mkdir -p ${log_dir}
#     mkdir -p ${output_dir}
#     tbl_name=$(date --date="$DATE" "+%d_%m_%Y")
#     kv="\${DATE}=${tbl_name};"
#     file_name="${DATE}_EDW_REPORT.gz"
#     log_file=${DATE}_EDW_REPORT.log
#     command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
#     echo "starting command: ${command}"
#     nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

# wait
# done
