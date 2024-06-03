#!/bin/bash

date=$1
reports_folder=/mnt/beegfs/share/tmp/archive_reports
feeds_folder=/mnt/beegfs/production/archived

for feed in AIR_ADJ_DA AIR_ADJ_MA AIR_REFILL_AC AIR_REFILL_DA AIR_REFILL_MA BUNDLE4U_GPRS BUNDLE4U_VOICE CB_SERV_MAST_VIEW CCN_GPRS_AC CCN_GPRS_DA CCN_GPRS_MA CCN_SMS_AC CCN_SMS_DA CCN_SMS_MA CCN_VOICE_AC CCN_VOICE_DA CCN_VOICE_MA Containers CUG_ACCESS_FEES DMC_DUMP_ALL DMC_DUMP_ALL_unsplitted EWP_FINANCIAL_LOG GGSN HSDP HSDP_tmp MAPS_INV_2G MAPS_INV_3G MAPS_INV_4G MOBILE_MONEY MSC SDP_ACC_ADJ_AC SDP_ACC_ADJ_MA SDP_ADJ_DA SDP_DMP_MA SGSN WBS_PM_RATED_CDRS SDP_DUMP_SUBSCRIBER SDP_DUMP_OFFER RECON; do

      echo "clean up $feed for day $date"
      echo "time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in $feeds_folder/$feed/$date/$date -out $reports_folder/$feed/$date  -nt 40  -vad -rmp $feeds_folder/$feed/ -tarPrefix "$feed-$date" -tf 1 -dp 15 -nosim"

      time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in $feeds_folder/$feed/$date/$date -out $reports_folder/$feed/$date  -nt 40  -vad -rmp $feeds_folder/$feed/ -tarPrefix "$feed-$date" -tf 1 -dp 15 -nosim

      echo "start archiving $feed for day $date"

      echo "time java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in $feeds_folder/$feed/$date/ -outarchive $feeds_folder/$feed/$date/$date  -nt 30 -cvzf -ts 50 -tarPrefix "$feed-$date" -out $reports_folder/$feed/$date -rgrm "$feeds_folder/$feed/"  -dp 15 -ll info -tf 1 -effl "^Archive-*.*$" -nosim"

      time java -Xmx30g -Xms30g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in $feeds_folder/$feed/$date/ -outarchive $feeds_folder/$feed/$date/$date  -nt 30 -cvzf -ts 50 -tarPrefix "$feed-$date" -out $reports_folder/$feed/$date -rgrm "$feeds_folder/$feed/"  -dp 15 -ll info -tf 1 -effl "^Archive-*.*$" -nosim

      echo "validate and delete archiving $feed for day $date"

      echo "time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in $feeds_folder/$feed/$date/$date -out $reports_folder/$feed/$date  -nt 40  -vad -rmp $feeds_folder/$feed/ -tarPrefix "$feed-$date" -tf 1 -dp 15 -nosim"

      time java -Xmx20g -Xms20g -Dlog4j.configurationFile=./log4j2.xml -jar FileOps_2.11-0.1-SNAPSHOT_archive.jar -in $feeds_folder/$feed/$date/$date -out $reports_folder/$feed/$date  -nt 40  -vad -rmp $feeds_folder/$feed/ -tarPrefix "$feed-$date" -tf 1 -dp 15 -nosim

     echo "push reports to hive table"
     ./moveToHDFS.sh $feed $date

     echo "move files to hdfs"
     ./movefilesToHDFS.sh $feed $date

done

