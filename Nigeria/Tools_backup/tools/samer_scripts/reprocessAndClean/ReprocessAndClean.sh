#!/bin/bash

dt=$(date -I -d "$2 + 1 day")
dt=$(date -d "$dt" +%Y-%m-%d)
#PROP_FILE="../conf/ValidationTool.conf"
#source $PROP_FILE
#echo $common_hdfs_path
commonHdfsPath=/FlareData/output_8
SORTED_DIR=$(hdfs dfs -ls $commonHdfsPath/$1 | grep "^d" | sort -k6,7  | tr -s ' ' | grep "$2\|$dt")
TBL_DT=""
#echo $SORTED_DIR
while read -r line; do
   #echo "....$line....."
#    echo $line | sed 's/.*=//'
    EX_DATE=$(echo $line | sed 's/.*=//')
    TBL_DT="$TBL_DT , $EX_DATE"
done <<< "$SORTED_DIR"
echo ${TBL_DT:2}

