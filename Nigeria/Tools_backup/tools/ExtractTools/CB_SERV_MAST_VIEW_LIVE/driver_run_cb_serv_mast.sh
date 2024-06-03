#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

date=$1
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/run_cb_serv_mast.sh ${date} ${date}  |tee /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/log/run_cb_serv_mast_$(date +%Y%m%d_%H%M%S).txt
