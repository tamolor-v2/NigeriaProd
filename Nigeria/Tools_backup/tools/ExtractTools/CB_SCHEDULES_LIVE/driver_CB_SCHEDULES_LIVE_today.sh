#!/bin/bash
bash /mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/CB_SCHEDULES_LIVE_today.sh 2>&1 |tee /mnt/beegfs/tools/ExtractTools/CB_SCHEDULES_LIVE/logs/CB_SCHEDULES_LIVE_extract_today_$(date +%Y%m%d_%H%M%S).txt

