#!/bin/bash
export JAVA_OPTS="-Xms3G -Xmx20G"
export LD_BIND_NOW=1
bash /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/exctract_PM_RATED_today_localjar_20191129_new.sh 2>&1 | tee /nas/share05/tools/ExtractTools/WBS_PM_RATED_CDRS/inc_log/exctract_PM_RATED_today_$(date +%Y%m%d_%H%M%S).log
