#!/bin/bash

perl /nas/share05/tools/SOLO_SCRIPTS/Daily_Solo_Script.pl $(date -d '-2 day' '+%Y%m%d') 2>&1  | tee /nas/share05/tools/SOLO_SCRIPTS/logs/Daily_Solo_Script_$(date +%Y%m%d_%H%M%S).txt
