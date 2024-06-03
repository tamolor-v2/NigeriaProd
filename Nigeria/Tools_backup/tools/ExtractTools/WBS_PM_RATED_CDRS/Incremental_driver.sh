#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
DELIMITER='|'
runDate=$(date +"%Y%m%d")
runTime=$(date +"%H%M%S")
MAX_SEQ=$4
HOUR=$(date +"%H")
cd ${WORK_DIR}
                log_dir=${WORK_DIR}/logs/${DATE}
                output_dir=${OUTPUT}/${DATE}/${HOUR}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}
                kv="\${date}=${DATE};\${maxSeq}=${MAX_SEQ};\${hr}=$HOUR"
                file_name=".${DATE}_WBS_PM_RATED_CDRS_${runDate}_${runTime}.gz"
                log_file=${DATE}_WBS_PM_RATED_CDRS_${runDate}_${runTime}.log
export JAVA_OPTS="-Xms3G -Xmx10G"
                command="bash ${WORK_DIR}/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/incremental_query_template.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"
                echo "starting command: ${command}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 

