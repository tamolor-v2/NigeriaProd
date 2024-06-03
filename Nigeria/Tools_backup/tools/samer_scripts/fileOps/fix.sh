#!/bin/bash

date=$1
prefix=$2

for feed in AIR_ADJ_DA AIR_ADJ_MA AIR_REFILL_AC AIR_REFILL_DA AIR_REFILL_MA BUNDLE4U_GPRS BUNDLE4U_VOICE CB_SERV_MAST_VIEW CCN_GPRS_AC CCN_GPRS_DA CCN_GPRS_MA CCN_SMS_AC CCN_SMS_DA CCN_SMS_MA CCN_VOICE_AC CCN_VOICE_DA CCN_VOICE_MA Containers CUG_ACCESS_FEES DMC_DUMP_ALL DMC_DUMP_ALL_unsplitted EWP_FINANCIAL_LOG GGSN HSDP HSDP_tmp MAPS_INV_2G MAPS_INV_3G MAPS_INV_4G MOBILE_MONEY MSC SDP_ACC_ADJ_AC SDP_ACC_ADJ_MA SDP_ADJ_DA SDP_DMP_MA SGSN WBS_PM_RATED_CDRS; do

      echo "clean up archiving $feed for day $date"

      echo "time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in /mnt/beegfs/share/production/archived/$feed/$date/$date -out /mnt/beegfs/share/tmp/archive_reports/$feed/$date  -nt 40  -vad -rmp /mnt/beegfs/share/production/archived/$feed/ -tarPrefix "$feed-$date-$prefix" -tf 1 -dp 15 -nosim"

      time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in /mnt/beegfs/share/production/archived/$feed/$date/$date -out /mnt/beegfs/share/tmp/archive_reports/$feed/$date  -nt 40  -vad -rmp /mnt/beegfs/share/production/archived/$feed/ -tarPrefix "$feed-$date-$prefix" -tf 1 -dp 15 -nosim

      echo "start archiving $feed for day $date"

      echo "time java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in /mnt/beegfs/share/production/archived/$feed/$date/ -outarchive /mnt/beegfs/share/production/archived/$feed/$date/$date  -nt 30 -cvzf -ts 50 -tarPrefix "$feed-$date-$prefix" -out /mnt/beegfs/share/tmp/archive_reports/$feed/$date -rgrm "/mnt/beegfs/share/production/archived/$feed/"  -dp 15 -ll info -tf 1 -effl "^Archive-*.*$" -nosim"

      time java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in /mnt/beegfs/share/production/archived/$feed/$date/ -outarchive /mnt/beegfs/share/production/archived/$feed/$date/$date  -nt 30 -cvzf -ts 50 -tarPrefix "$feed-$date-$prefix" -out /mnt/beegfs/share/tmp/archive_reports/$feed/$date -rgrm "/mnt/beegfs/share/production/archived/$feed/"  -dp 15 -ll info -tf 1 -effl "^Archive-*.*$" -nosim

      echo "validate and delete archiving $feed for day $date"

      echo "time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in /mnt/beegfs/share/production/archived/$feed/$date/$date -out /mnt/beegfs/share/tmp/archive_reports/$feed/$date  -nt 40  -vad -rmp /mnt/beegfs/share/production/archived/$feed/ -tarPrefix "$feed-$date-$prefix" -tf 1 -dp 15 -nosim"

      time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in /mnt/beegfs/share/production/archived/$feed/$date/$date -out /mnt/beegfs/share/tmp/archive_reports/$feed/$date  -nt 40  -vad -rmp /mnt/beegfs/share/production/archived/$feed/ -tarPrefix "$feed-$date-$prefix" -tf 1 -dp 15 -nosim


done

