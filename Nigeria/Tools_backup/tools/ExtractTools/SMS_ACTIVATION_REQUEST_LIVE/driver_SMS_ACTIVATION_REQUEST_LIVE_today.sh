#!/bin/bash
bash /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/SMS_ACTIVATION_REQUEST_LIVE_today.sh 2>&1 |tee /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/logs/SMS_ACTIVATION_REQUEST_LIVE_extract_today_$(date +%Y%m%d_%s).txt

