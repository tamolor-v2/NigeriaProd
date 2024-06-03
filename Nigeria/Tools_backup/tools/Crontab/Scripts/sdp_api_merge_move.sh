#!/bin/bash
#
mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
removeDir=$(date -d '-60 day' '+%Y%m%d')
mkdir -p /mnt/beegfs/tools/Crontab/logs/merge_sdp/${currDate}
rm -r /mnt/beegfs/tools/Crontab/logs/merge_sdp/${removeDir}
mkdir -p /mnt/beegfs/tools/Crontab/logs/move_sdp_merge/${currDate}
rm -r /mnt/beegfs/tools/Crontab/logs/move_sdp_merge/${removeDir}

# merge sdp files
echo "start merging"
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/kpi_reports/scripts/filOps/log4j2.xml -jar /mnt/beegfs/tools/kpi_reports/scripts/filOps/FileOps_2.11-0.1-SNAPSHOT.jar -in /mnt/beegfs/live/SDP_API/incoming/ -marge /mnt/beegfs/live/SDP_API/incoming_marge/ /mnt/beegfs/production/archived/SDP_API/  -out /mnt/beegfs/tools/Crontab/logs/merge_sdp/${currDate} -nt 32 -nosim -od -of -tf 1

# move merged files to the right incoming file
echo "start moving"
time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/live/SDP_API/incoming_marge  -out /mnt/beegfs/tools/Crontab/logs/move_sdp_merge/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/live/SDP_API_MERGED -re -rg "^.*new_sag_sag_interface_full_.*([0-9]{8})([0-9]{2})([0-9]{7}).*$"  -opp "|@1/|@2" -nosim -lbl move_merge_incoming_SDP_API_ 2>&1 | tee "/mnt/beegfs/tools/Crontab/logs/move_sdp_merge/${currDate}/move_live_proces_arch_SDP_API_$mytime.log"
