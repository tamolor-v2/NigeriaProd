#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/exctract_PM_RATED_yesterday_last_run_localjar_20190417_D_2.sh 2>&1 | tee /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/inc_log/exctract_PM_RATED_D_2_$(date +%Y%m%d_%H%M%S).log
