#! /bin/bash
for i in {1..3}
do
bash /mnt/beegfs/tools/MSC_DAAS_LEA/LEA.scala "--date" $(date -d '-{i} day' '+%Y%m%d') 2>&1 | tee /mnt/beegfs/tools/MSC_DAAS_LEA/logs/LEA_prev_$(date +"%Y%m%d")_$(date +"%H%M%S").log
done
