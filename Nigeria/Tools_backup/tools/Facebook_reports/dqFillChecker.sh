

DATE=${1}
counter=0
SYS_DATE=$(date '+%Y-%m-%d %H:%m:%S')
schema=${2}
hasCoverageMaps=$3
partition=""

if [ $hasCoverageMaps == "daily" ]
then 
  aFeeds=(FB_CELL_QOS_4G_DQ_RPT FB_CELL_QOS_3G_DQ_RPT FB_CELL_QOS_2G_DQ_RPT)
else
  aFeeds=(FB_FEED_SLA_DQ_MONTHLY FB_COVERAGE_DQ_RPT_DETAIL)
fi

for feed in "${aFeeds[@]}"
do 
  if [ $feed == FB_COVERAGE_DQ_RPT_DETAIL ]
  then
   partition="report_month"
  else
   partition="tbl_dt"
  fi
  
  q1=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5  --schema ${schema} --execute "select count(*) from ${feed} where ${partition} >= ${DATE}")
  q11=$(echo $q1 |  sed "s/\"//g")
  if [ $q11 != 0 ]
  then
    echo "$SYS_DATE -- $feed table has $q11 records!"
  else
    echo "$feed has no data!"
    counter=$((counter+1))
  fi	
done

## checking if all tables has data..
if [ $counter == 0 ]
then
  echo "Moving to the next task.."
else
  echo -e "Error: One -or more- of the above tables has no data! Check the above log to see which one.\nJob Failed!"
  exit 1;
fi
