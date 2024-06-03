#! /bin/bash
yest=$(date -d '-1 day' '+%Y%m%d')
mytime() {
date +"%Y%m%d"
}
prev2Days=$(date -d "$yest -1 days" +'%Y%m%d')
prev3Days=$(date -d "$prev2Days -1 days" +'%Y%m%d')
BslDates="$yest,$prev2Days,$prev3Days"
echo "$BslDates"
