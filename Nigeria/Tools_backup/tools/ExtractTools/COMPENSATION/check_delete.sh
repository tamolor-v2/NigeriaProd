yest=$(date -d "-1 day" '+%Y%m%d')
for i in {1..120}; do
load_date=$(hadoop fs -ls hdfs://ngdaas/FlareData/output_8/COMPENSATION | awk '{print $6}')
today=$(date +'%Y-%m-%d')
if [[ "$load_date" == *"$today"* ]]; then
dtd=$yest
yesterday=$( date -d "${dtd} -41 days" +'%Y%m%d' )
num_days=41
for i in `seq 1 $num_days`
do
date=$(date +%Y%m%d -d "${yesterday}+${i} days")
echo $date # Use this however you want!
hadoop fs -ls  hdfs://ngdaas/FlareData/output_8/COMPENSATION/tbl_dt=${date}    |   tr -s " "    |    cut -d' ' -f6-8    |     grep "^[0-9]"    |    awk 'BEGIN{ MIN=480; LAST=60*MIN; "date +%s" | getline NOW } { cmd="date -d'\''"$1" "$2"'\'' +%s"; cmd | getline WHEN; DIFF=NOW-WHEN; if(DIFF > LAST){ print "Deleting: "$3; system("hadoop fs -rm -r "$3) }}'
done
fi

#hive -e "msck repair table flare_8.COMPENSATION;"

sleep 30
hive -e "msck repair table flare_8.COMPENSATION;"

done
