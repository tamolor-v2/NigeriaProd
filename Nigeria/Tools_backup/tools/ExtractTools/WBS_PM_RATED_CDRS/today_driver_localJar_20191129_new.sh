#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"


WORK_DIR=$1
OUTPUT=$2
DATE=$3
PREV=$(date -d "${DATE} -1 day" +"%Y%m%d")
hr=$(date +'%H')
ms=$(date +'%M%S')
export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
DELIMITER='|'
cd ${WORK_DIR}
                log_dir=${WORK_DIR}/logs/${DATE}/${hr}
                output_dir=${OUTPUT}/${DATE}
                mkdir -p ${log_dir}
                mkdir -p ${output_dir}


                kv="\${date}=${DATE};\${prev}=${PREV}"
                file_name=".${DATE}_WBS_PM_RATED_CDRS_TODAY_${hr}${ms}.gz"
                log_file=${DATE}_WBS_PM_RATED_CDRS_${hr}_TODAY.log

                command="bash /home/daasuser/JDBC_Tool/JDBCExtract.scala --configFile ${WORK_DIR}/config.properties --sqlFile ${WORK_DIR}/today_query_template_20191129.sql --outputFile ${output_dir}/${file_name} --delimiter ${DELIMITER} --kvTemplate ${kv}"

                echo "starting command: ${command}"
                nohup  ${command} >  ${log_dir}/${log_file}  2>&1 &

wait
retVal=$?
echo "driver retval= $retVal"
if [ $retVal -eq 0 ];
then
exit 0
else
exit 5 
fi
