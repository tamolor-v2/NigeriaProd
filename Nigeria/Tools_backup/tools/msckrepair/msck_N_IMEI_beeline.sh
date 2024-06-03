ts=$(date +"%Y%m%d_%H%M%S")
echo "running msck_repair at $ts">>/home/daasuser/last_msck_run.log
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "set hive.msck.path.validation =ignore;"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.TBL_DATA_AGENT_REGISTRATION_VW;"

