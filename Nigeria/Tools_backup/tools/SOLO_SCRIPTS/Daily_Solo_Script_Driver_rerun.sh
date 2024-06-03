#!/bin/bash

perl /nas/share05/tools/SOLO_SCRIPTS/Daily_Solo_Script.pl $1 2>&1  | tee /nas/share05/tools/SOLO_SCRIPTS/logs/Daily_Solo_Script_$(date +%Y%m%d_%H%M%S).txt
