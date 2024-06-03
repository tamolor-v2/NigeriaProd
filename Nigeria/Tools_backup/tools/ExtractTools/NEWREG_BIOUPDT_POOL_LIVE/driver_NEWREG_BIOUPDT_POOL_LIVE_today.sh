#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

bash /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/NEWREG_BIOUPDT_POOL_LIVE_today.sh 2>&1 |tee /mnt/beegfs_bsl/tools/ExtractTools/NEWREG_BIOUPDT_POOL_LIVE/logs/NEWREG_BIOUPDT_POOL_LIVE_extract_today_$(date +%Y%m%d_%s).txt
