#! /bin/bash
yest=$(date -d '-2 day' '+%Y%m%d')

mytime() {
date +"%Y%m%d"
}
schemaName=$1
echo "$schemaName"
while read LINE
do
echo "drop table $1.$LINE"
#mv  $LINE
done< $2
