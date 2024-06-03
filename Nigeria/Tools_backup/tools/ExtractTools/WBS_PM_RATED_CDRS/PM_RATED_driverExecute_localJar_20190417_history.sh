#!/bin/bash
export LD_BIND_NOW=1
bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/exctract_PM_RATED_today_localjar_20190417_history.sh "$1" 2>&1 | tee /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/inc_log/exctract_PM_RATED_history_${1}_$(date +%Y%m%d_%H%M%S).log
