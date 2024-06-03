FEED_NAME=$1
DATA_PATH="/FlareData/output_8"
SS_DATA_PATH="/FlareData/CAAS_SS"
DATA_NAMESPACE="stg_gen"
ss_dt=$2

kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM

echo "$FEED_NAME"

hadoop fs -mkdir -p ${SS_DATA_PATH}/${FEED_NAME}/ss_dt=${ss_dt}
hadoop fs -mv ${DATA_PATH}/${FEED_NAME}/* ${SS_DATA_PATH}/${FEED_NAME}/ss_dt=${ss_dt}
 
#sed -i "1s/^/drop table ${DATA_NAMESPACE}.${FEED_NAME}; \\n /" /mnt/beegfs_bsl/Deployment/DEV/output/${FEED_NAME}/${FEED_NAME}.hql
#echo "; msck repair table ${DATA_NAMESPACE}.${FEED_NAME};" >> /mnt/beegfs_bsl/Deployment/DEV/output/${FEED_NAME}/${FEED_NAME}.hql

echo "hive -hivevar DATA_NAMESPACE=${DATA_NAMESPACE} -hivevar DATA_PATH=${DATA_PATH} -f /mnt/beegfs_bsl/Deployment/DEV/output/${FEED_NAME}/${FEED_NAME}.hql"
hive -hivevar DATA_NAMESPACE=${DATA_NAMESPACE} -hivevar DATA_PATH=${DATA_PATH} -f /mnt/beegfs_bsl/Deployment/DEV/output/${FEED_NAME}/${FEED_NAME}.hql
