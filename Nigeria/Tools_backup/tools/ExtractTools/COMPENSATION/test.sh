hadoop fs -ls  hdfs://ngdaas/FlareData/output_8/COMPENSATION/tbl_dt=20190122    |   tr -s " "    |    cut -d' ' -f6-8    |     grep "^[0-9]"    |    awk 'BEGIN{ MIN=1440; LAST=60*MIN; "date +%s" | getline NOW } { cmd="date -d'\''"$1" "$2"'\'' +%s"; cmd | getline WHEN; DIFF=NOW-WHEN; if(DIFF > LAST){ print "Deleting: "$3; }}'

