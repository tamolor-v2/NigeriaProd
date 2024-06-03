#!/bin/bash

currdate=$1
filetime=$(date +%Y%m%d_%s)
run_date=$(date +"%Y%m%d")
if [[ -z currdate || $# -eq 0  ]]; then
  echo 'rundate is not set .... Exitting'
  exit 1
fi

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM

mkdir  -p /mnt/beegfs/tools/SupportTeam/moving/${currdate}/

hdfs dfs -mkdir -p /user/ng_ops/LZ_RECON/f_name=CCN_GPRS_MA/tbl_dt=${currdate}/

 zgrep '/mnt/beegfs_bsl/live/' /mnt/beegfs_bsl/production/movefromlocaltodfs/report/CCN_GPRS_MA/${currdate}/fileinfo*.txt.gz  |awk -F '|' '{print $3"," $7"," $14"," $16"," $17 }' |awk -F '/' '{print $1 "/" $2 "/" $3 "/" $4 "/" $5 "/" $6 "/" $7 "/" $8 "/" $9 "/"$10 "/"$11 "/"$12 "/" $13 "," $11 }' > /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_CCN_GPRS_MA_${filetime}.txt
hadoop fs -rm /user/ng_ops/LZ_RECON/f_name=CCN_GPRS_MA/tbl_dt=${currdate}/*
hdfs dfs -put /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_CCN_GPRS_MA_${filetime}.txt /user/ng_ops/LZ_RECON/f_name=CCN_GPRS_MA/tbl_dt=${currdate}/

hdfs dfs -mkdir -p /user/ng_ops/LZ_RECON/f_name=CS6_UNIFIED/tbl_dt=${currdate}/

zgrep '/mnt/beegfs_bsl/live/' /mnt/beegfs_bsl/production/movefromlocaltodfs/report/CS6_Unified/${currdate}/fileinfo*.txt.gz |awk -F '|' '{print $3"," $7"," $14"," $16"," $17 }' |awk -F '/' '{print $1 "/" $2 "/" $3 "/" $4 "/" $5 "/" $6 "/" $7 "/" $8 "/" $9 "/"$10 "/"$11 "/"$12 "/" $13  "," $13 }'  > /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_CS6_UNIFIED${filetime}.txt
hadoop fs -rm /user/ng_ops/LZ_RECON/f_name=CS6_UNIFIED/tbl_dt=${currdate}/*
hdfs dfs -put /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_CS6_UNIFIED_${filetime}.txt /user/ng_ops/LZ_RECON/f_name=CS6_UNIFIED/tbl_dt=${currdate}/

hdfs dfs -mkdir -p /user/ng_ops/LZ_RECON/f_name=DPI/tbl_dt=${currdate}/

zgrep '/mnt/beegfs_bsl/live/' /mnt/beegfs_bsl/production/movefromlocaltodfs/report/DPI/${currdate}/fileinfo*.txt.gz  |awk -F '|' '{print $3"," $7"," $14"," $16"," $17 }' |awk -F '/' '{print $1 "/" $2 "/" $3 "/" $4 "/" $5 "/" $6 "/" $7 "/" $8 "/" $9 "/"$10 "/"$11 "/"$12 "/" $13 "," $11 }' > /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_DPI_${filetime}.txt
hadoop fs -rm /user/ng_ops/LZ_RECON/f_name=DPI/tbl_dt=${currdate}/*
hdfs dfs -put /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_DPI_${filetime}.txt /user/ng_ops/LZ_RECON/feed_name=DPI/tbl_dt=${currdate}/

hdfs dfs -mkdir -p /user/ng_ops/LZ_RECON/f_name=GGSN/tbl_dt=${currdate}/

zgrep '/mnt/beegfs_bsl/live/' /mnt/beegfs_bsl/production/movefromlocaltodfs/report/GGSN/${currdate}/fileinfo*.txt.gz |awk -F '|' '{print $3"," $7"," $14"," $16"," $17 }' |awk -F '/' '{print $1 "/" $2 "/" $3 "/" $4 "/" $5 "/" $6 "/" $7 "/" $8 "/" $9 "/"$10 "/"$11 "/"$12 "/" $13 "," $11 }' > /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_GGSN_${filetime}.txt
hadoop fs -rm /user/ng_ops/LZ_RECON/f_name=GGSN/tbl_dt=${currdate}/*
hdfs dfs -put /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_GGSN_${filetime}.txt /user/ng_ops/LZ_RECON/f_name=GGSN/tbl_dt=${currdate}/

hdfs dfs -mkdir -p /user/ng_ops/LZ_RECON/f_name=EXCLUDE/tbl_dt=${currdate}/

zgrep '/mnt/beegfs_bsl/live/' /mnt/beegfs_bsl/production/movefromlocaltodfs/report/Exclude/${currdate}/fileinfo*.txt.gz |awk -F '|' '{print $3"," $7"," $14"," $16"," $17 }' |awk -F '/' '{print $1 "/" $2 "/" $3 "/" $4 "/" $5 "/" $6 "/" $7 "/" $8 "/" $9 "/"$10 "/"$11 "/"$12 "/" $13  "," $12 }' > /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_EXCLUDE_${filetime}.txt
hadoop fs -rm /user/ng_ops/LZ_RECON/f_name=EXCLUDE/tbl_dt=${currdate}/*
hdfs dfs -put /mnt/beegfs/tools/SupportTeam/moving/${currdate}/${run_date}_EXCLUDE_${filetime}.txt /user/ng_ops/LZ_RECON/f_name=EXCLUDE/tbl_dt=${currdate}/

#rm -r /mnt/beegfs/tools/SupportTeam/moving/${currdate}/*