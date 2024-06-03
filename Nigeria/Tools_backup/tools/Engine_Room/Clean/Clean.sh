kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM

declare -a List=(
"CDR_DATA"
"EVD_STOCK_LEVEL"
"EVD_TRANSACTIONS"
"MFS_ACCOUNTS"
"MFS_ACQUISITION"
"MFS_REGISTRATIONS"
"MFS_STOCK_LEVEL"
"SIM_TRANSACTIONS"
"CDR_RECHARGE"
)

for Feed in "${List[@]}"

 do 

# Specify the retention policy from clean_config table

retention_policy=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select retention from flare_8.clean_config where feed_name = '$Feed'" --output-format TSV |  tr '\t' ',')

# Specify the retention unit (day, month, year)

unit=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select unit from flare_8.clean_config where feed_name = '$Feed'" --output-format TSV |  tr '\t' ',')

Date=$(date -d "-$retention_policy $unit" '+%Y%m%d')

echo $retention_policy $unit
echo $Feed
echo $Date

# Delete the data from presto

delete_command=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select concat('delete from ',schema_name,'.',feed_name,' where ',partition_column,'=','$Date',';') from flare_8.clean_config where feed_name = '$Feed'" --output-format TSV |  tr '\t' ',')

echo $delete_command

/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "$delete_command"

# Delete the data from HDFS

delete_HDFS=$(/opt/presto/bin/presto --server 10.1.197.145:8999 --catalog hive5 --schema flare_8 --execute "select concat('hadoop fs -rm -r ', hdfs_path, '/', partition_column, '=', '$Date') from flare_8.clean_config where feed_name = '$Feed'"  --output-format TSV |  tr '\t' ',') 

echo $delete_HDFS

$delete_HDFS

 done
