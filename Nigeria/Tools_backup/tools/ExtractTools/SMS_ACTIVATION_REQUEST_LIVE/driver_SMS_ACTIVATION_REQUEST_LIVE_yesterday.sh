#!/bin/bash
bash /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/extract_SMS_ACTIVATION_REQUEST_LIVE_yesterday.sh 2>&1 |tee /mnt/beegfs/tools/ExtractTools/SMS_ACTIVATION_REQUEST_LIVE/logs/SMS_ACTIVATION_REQUEST_LIVE_extract_yesterday_$(date +%Y%m%d_%s).txt
