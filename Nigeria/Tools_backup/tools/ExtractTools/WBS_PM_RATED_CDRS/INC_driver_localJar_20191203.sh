#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
hr=$(date +'%H')
ms=$(date +'%M%S')
A_NUM_FILTER=($4)
MAX_SEQ=$5
CURR_MAX_SEQ=$6
export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
DELIMITER='|'
cd ${WORK_DIR}
        for anum in "${A_NUM_FILTER[@]}"; do
                echo "work on anum: ${anum}"
                log_dir=${WORK_DIR}/logs/${DATE}/${hr}
                output_dir=${OUTPUT}/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}


                kv="\${date}=${DATE};\${anum}=${anum};\${maxSeq}=${MAX_SEQ};\${currmaxSeq}=${CURR_MAX_SEQ}"
                file_name=".${DATE}_WBS_PM_RATED_CDRS_${anum}_${hr}${ms}.gz"
                log_file=${DATE}_WBS_PM_RATED_CDRS_${hr}_${anum}.log

                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config_20191203.properties --sqlFile ${WORK_DIR}/INC_query_template_20191203.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

                echo "starting command: ${command}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

        done
wait
retVal=$?
echo "driver retval= $retVal"
if [ $retVal -eq 0 ];
then
exit 0
else
exit 5 
fi
