#!/bin/bash
working_folder=$1
incoming_folder=$2
extract_folder="${working_folder}/tmp"

date=$(date +"%Y%m%d")

mkdir /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/logs/$(date +%Y%m%d_%H)


for i in {0..9}
do
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/CB_SERV_MAST_VIEW_LIVE_dec_last_run.sh ${i} $1 |tee /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/logs/$(date +%Y%m%d_%H)/CB_SERV_MAST_VIEW_LIVE_extract_today_${i}_$(date +%Y%m%d_%s).txt & 
done
bash /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/CB_SERV_MAST_VIEW_LIVE_Decode_last_run.sh $1 |tee /mnt/beegfs_bsl/tools/ExtractTools/CB_SERV_MAST_VIEW_LIVE/logs/$(date +%Y%m%d_%H)/CB_SERV_MAST_VIEW_LIVE_extract_today_10_$(date +%Y%m%d_%s).txt &
