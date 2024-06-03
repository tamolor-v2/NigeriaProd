kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM

Feed=$1
Date=$2

retention_policy=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select retention from flare_8.clean_config where feed_name = '$Feed'" --output-format TSV |  tr '\t' ',')

unit=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select unit from flare_8.clean_config where feed_name = '$Feed'" --output-format TSV |  tr '\t' ',')

echo $retention_policy
echo $Feed
echo $Date

delete_command=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select concat('delete from ',schema_name,'.',feed_name,' where ',partition_column,'=','$Date',';') from flare_8.clean_config where feed_name = '$Feed'" --output-format TSV |  tr '\t' ',')

echo $delete_command

/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "$delete_command"

delete_HDFS=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select concat('hadoop fs -rm -r ', hdfs_path, '/', partition_column, '=', '$Date') from flare_8.clean_config where feed_name = '$Feed'"  --output-format TSV |  tr '\t' ',') 

echo $delete_HDFS

#$delete_HDFS

