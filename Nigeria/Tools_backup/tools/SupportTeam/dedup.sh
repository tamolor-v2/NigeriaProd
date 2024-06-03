yest=$(date -d '-1 day' '+%Y%m%d')

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_VTU_DUMP --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/CS5_VTU_DUMP_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_SMS_MA --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/SMS_MA_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_SMS_DA --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/SMS_DA_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_SMS_AC --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/SMS_AC_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_VOICE_AC --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/VOICE_AC_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_VOICE_DA --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/VOICE_DA_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_VOICE_MA --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/VOICE_MA_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_GPRS_AC --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/GPRS_AC_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_GPRS_MA --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/GPRS_MA_$(date +%Y%m%d_%s).txt

bash /mnt/beegfs/tools/DedupTools/Dedup_V5.scala --configFile /mnt/beegfs/tools/DedupTools/dedup.conf --feed CS5_CCN_GPRS_DA --date ${yest} | tee /mnt/beegfs/tools/SupportTeam/logs/DEDUP/GPRS_DA_$(date +%Y%m%d_%s).txt

