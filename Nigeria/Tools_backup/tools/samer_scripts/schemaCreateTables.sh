#! /bin/bash
yest=$(date -d '-2 day' '+%Y%m%d')
now=$(date +%Y%m%d)
mytime() {
date +"%Y%m%d"
}
schemaName=$1
sqlStatement=
hive -S -e "use $1;show tables;" >${schemaName}_TablesList.dat
echo "use $1;" >${schemaName}_allTables_${now}.hql
while read table
do
#echo "le $1.$LINE"
echo "show create table ${table};" >>${schemaName}_allTables_${now}.hql
#mv  $LINE
done< ${schemaName}_TablesList.dat
hive  -S -f  ./${schemaName}_allTables_${now}.hql >allCreateStatements_${schemaName}_${now}.sql
