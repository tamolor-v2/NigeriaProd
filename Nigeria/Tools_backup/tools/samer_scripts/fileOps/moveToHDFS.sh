#!/bin/bash

ROOT_DIR=/mnt/beegfs/share/tmp/archive_reports
FEED_NAME=$1
FEED_DATE=$2
HDFS_DIR=/user/daasuser/FileOpsReport
echo "hdfs dfs -mkdir -p $HDFS_DIR/ArchivedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE"
hdfs dfs -mkdir -p $HDFS_DIR/ArchivedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE
echo "hdfs dfs -mkdir -p $HDFS_DIR/DeltetedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE"
hdfs dfs -mkdir -p $HDFS_DIR/DeltetedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE
echo "hdfs dfs -put $ROOT_DIR/$FEED_NAME/$FEED_DATE/archiveReport* $HDFS_DIR/ArchivedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE"
hdfs dfs -put $ROOT_DIR/$FEED_NAME/$FEED_DATE/archiveReport* $HDFS_DIR/ArchivedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE
echo "hdfs dfs -put $ROOT_DIR/$FEED_NAME/$FEED_DATE/deltetedReport* $HDFS_DIR/DeltetedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE"
hdfs dfs -put $ROOT_DIR/$FEED_NAME/$FEED_DATE/deltetedFilesReport* $HDFS_DIR/DeltetedReport/feed_name=$FEED_NAME/tbl_dt=$FEED_DATE

