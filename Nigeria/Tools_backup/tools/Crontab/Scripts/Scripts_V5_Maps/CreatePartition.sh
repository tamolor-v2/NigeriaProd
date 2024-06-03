#!/bin/bash
hdfs_dir=/FlareData/output_8
db_name=flare_8
number_of_days=7
start_date=$(date -d '+1 day' '+%Y%m%d')
for feed in  MAPS_2G_CELL_D MAPS_2G_CELL_W_BH MAPS_3G_CELL_D MAPS_3G_CELL_D_BH MAPS_3G_CELL_W MAPS_3G_CELL_M MAPS_3G_CELL_M_BH MAPS_3G_CN_D MAPS_3G_CN_D_BH MAPS_3G_CN_W_BH MAPS_3G_CN_M MAPS_3G_CN_W MAPS_2G_CN_M_DATA_BH MAPS_2G_CN_W_BH
do
  kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
  echo $feed
  for ((i=0;i<${number_of_days};i++)) 
  do
    day=`date --date="${start_date} +$i day" +%Y%m%d`
    echo "hdfs dfs -mkdir ${hdfs_dir}/${feed}/tbl_dt\=$day"
    hdfs dfs -mkdir ${hdfs_dir}/${feed}/tbl_dt\=$day
  done
  echo "msck repair table ${db_name}.$feed"
  hive -e "msck repair table ${db_name}.$feed"
done
