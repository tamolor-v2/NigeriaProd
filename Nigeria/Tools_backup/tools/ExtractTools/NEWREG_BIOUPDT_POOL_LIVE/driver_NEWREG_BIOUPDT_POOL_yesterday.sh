#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

bash /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/extract_NEWREG_BIOUPDT_POOL_LIVE_yesterday.sh 2>&1 |tee /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/logs/NEWREG_BIOUPDT_POOL_LIVE_extract_yesterday_$(date +%Y%m%d_%s).txt
