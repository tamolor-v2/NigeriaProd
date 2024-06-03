#! /bin/bash
bash /mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_CDRS/exctract_PM_RATED_Prev.sh $(date -d '-${1} day' '+%Y%m%d') 2>&1 | tee /mnt/beegfs/tools/ExtractTools/WBS_PM_RATED_CDRS/tt.log
