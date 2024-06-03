mytime=$(date +"%Y-%m-%d_%H-%M-%S")
currDate=$(date +"%Y%m%d")
removeDir=$(date -d '-60 day' '+%Y%m%d')
generalTime=$(date +"%Y-%m-%d %H:%M:%S")
generalLogs=/mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/general_logs
#move All_Processed_feeds
mkdir -p /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/move/${currDate}
rm -r /mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/move/${removeDir}
mkdir -p  ${generalLogs}/${currDate}
time java -Xmx30g -Xms20g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/archived/SDP_PAM_ALL -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 16 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/CS5_SDP_PAM_ALL/ -re -rg "^SDPCDR.*_([0-9]{8})-([0-9]{2}).*$" -opp "|@1/|@2/"  -nosim -lbl move_live_processed_arch_CS5_SDP_PAM_ALL 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/V5_1_Feeds/move/${currDate}/move_live_proces_arch_CS5_SDP_PAM_ALL_$mytime.log"
