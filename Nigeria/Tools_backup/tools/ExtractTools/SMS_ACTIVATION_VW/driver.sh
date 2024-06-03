#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3 
DELIMITER='|'
cd ${WORK_DIR}
log_dir=${WORK_DIR}/logs/${DATE}
output_dir=${OUTPUT}/${DATE}
mkdir -p ${log_dir}
mkdir -p ${output_dir}

kv="\${DATE}=${DATE};"
file_name="${DATE}_SMS_ACTIVATION_VW.gz"
log_file=${DATE}_SMS_ACTIVATION_VW.log
command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

echo "starting command: ${command}"
nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

wait

# #!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

# WORK_DIR=$1
# OUTPUT=$2
# DATE=$3 
# # HOURS=($4)
# #A_NUM_FILTER=($5)
# DELIMITER='|'
# cd ${WORK_DIR}

# for i in {25..25}; do
#     DATE=201902$i
#     echo $DATE

#     log_dir=${WORK_DIR}/logs/${DATE}
#     output_dir=${OUTPUT}/${DATE}
#     mkdir -p ${log_dir}
#     mkdir -p ${output_dir}

#     kv="\${DATE}=${DATE};"
#     file_name="${DATE}_SMS_ACTIVATION_VW.gz"
#     log_file=${DATE}_SMS_ACTIVATION_VW.log
#     command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
#     echo "starting command: ${command}"
#     nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

# wait
# done
