#! /bin/bash
bash /mnt/beegfs/tools/MSC_DAAS_LEA/LEA.scala "--date" $(date +"%Y%m%d") 2>&1 | tee /mnt/beegfs/tools/MSC_DAAS_LEA/logs/LEA_$(date +"%Y%m%d")_$(date +"%H%M%S").log
