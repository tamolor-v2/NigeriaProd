#! /bin/bash
bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/extract_PM_RATED_Prev_localJar.sh $(date -d '-1 day' '+%Y%m%d') 2>&1 | tee /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/logs/extract_PM_RATED_Prev_d_1_$(date +"%Y%m%d")_$(date +"%H%M%S").log

