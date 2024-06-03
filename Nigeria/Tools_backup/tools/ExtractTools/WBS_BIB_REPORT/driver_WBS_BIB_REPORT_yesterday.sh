#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

bash /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/extract_WBS_BIB_REPORT_yesterday.sh 2>&1 |tee /mnt/beegfs_bsl/tools/ExtractTools/WBS_BIB_REPORT/logs/WBS_BIB_REPORT_extract_yesterday_$(date +%Y%m%d_%s).txt
