#!/bin/bash
#move All_Processed_feeds
#move MOBILE_MONEY
#------------------
bash /home/daasuser/samer_scripts/mobileMoney_rename.sh
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/samer_scripts/fileOps/FileOps_Lable.jar -in /home/daasuser/samer_scripts/MOBILE_MONEY/processed/ -out /mnt/beegfs/tmp/waddah/move -od -tf 10 -nt 32 -dp 15 -of -op list -mv /home/daasuser/samer_scripts/archived/MOBILE_MONEY -rg  "^([0-9]{8})(DAILY_TRANSACTIONS).*$" -opp "|@1" -iffl "^201[7|8|9]" -nosim -lbl move_live_proces_arch_MOBILE_MONEY_ 2>&1 | tee "/mnt/beegfs/tmp/waddah/move/move_live_proces_archMOBILE_MONEY_$mytime.log"


#List incoming files in live cluster
#--------------------------
#mytime=$(date +"%Y-%m-%d_%H-%M-%S")
#time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/home/daasuser/fileOps/log4j2.xml -jar /home/daasuser/fileOps/FileOps_Lable.jar -in /mnt/beegfs/live/  -out /mnt/beegfs/tmp/waddah/list -od -tf 10 -nt 32 -dp 15 -of -op list -lbl listIncomingLive 2>&1 | tee "/mnt/beegfs/tmp/waddah/list/list_live_Incoming$(mytime).log"
