hive -e "drop table audit.$1"
hive -f /mnt/beegfs/tools/ValidationTool_Presto/sql_scripts/tbls_ddl/$1.hql
hive -e "msck repair table audit.$1"
