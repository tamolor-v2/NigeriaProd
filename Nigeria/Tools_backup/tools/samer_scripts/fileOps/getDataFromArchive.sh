#!/bin/bash

feed=$1
day=$2

HDFS_DIR=/archive/$feed/$day
SHARE_DIR=/mnt/beegfs/share/production/archived/$feed

echo "get data from $HDFS_DIR"
echo "hdfs dfs -get  $HDFS_DIR $SHARE_DIR/$day/$day"
mkdir -p $SHARE_DIR/$day/$day
hdfs dfs -get  $HDFS_DIR $SHARE_DIR/$day

echo "untar files in $SHARE_DIR/$day/$day"
for filename in $SHARE_DIR/$day/$day/*.tar.gz
do
  echo "untar file $filename"
  tar -xvzf $filename -C $SHARE_DIR/
done

