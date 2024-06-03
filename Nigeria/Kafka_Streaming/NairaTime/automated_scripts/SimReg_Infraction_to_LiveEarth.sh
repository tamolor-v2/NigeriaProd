#!bin/bash

beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.CB_NEWREG_BIOUPDT_POOL_HOURLY;"

Start_Date=$(date '+%Y%m%d')
End_Date=$(date '+%Y%m%d')
Start_Hr=$(date -d '-1 hour' '+%H')
End_Hr=$(date -d '-1 hour' '+%H')

q=$(/opt/presto/bin/presto --server master01003:8999 --catalog hive5 --schema flare_8 --execute "select count(*) from FLARE_8.CB_NEWREG_BIOUPDT_POOL_HOURLY where tbl_dt between $Start_Date and $End_Date and cast(Date_format(from_unixtime(UPDATED_DT/1000),'%H') as integer) between $Start_Hr and $End_Hr")
q11=$(echo $q |  sed "s/\"//g")
   if [ $q11 != 0 ]
   then
    echo "The date $End_Date and hour $End_Hr has data on flare_8.CB_NEWREG_BIOUPDT_POOL_HOURLY! Continue processing.."

BASE_LOCATION=/nas/share05/tools/DBExtrct
KAFKA_SECURITY="-Djava.security.auth.login.config=/export/home/daasuser/Kamanja/config/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
java -Dlog4j.configurationFile=${BASE_LOCATION}/conf/log4j2.xml \
${KAFKA_SECURITY} \
-cp ${BASE_LOCATION}/lib/ExtDependencyLibs_2.11.jar:${BASE_LOCATION}/lib/ExtDependencyLibs2_2.11.jar:${BASE_LOCATION}/lib/KamanjaInternalDeps_2.11.jar:${BASE_LOCATION}/lib/dbextract_2.11.jar:${BASE_LOCATION}/lib/dbutils_2.11.jar:${BASE_LOCATION}/lib/kafka-clients.jar:${BASE_LOCATION}/lib/presto-jdbc.jar  com.ligadata.utils.dbextract.DBExtract  -cfg  ${BASE_LOCATION}/conf/conf.json -s liveearth_simreg -sd $Start_Date -ed $End_Date -sh $Start_Hr -eh $End_Hr -p 2 -cn  presto1

   else
    echo "Inserting data to FLARE_8.CB_NEWREG_BIOUPDT_POOL_HOURLY"
bash /nas/share05/tools/ExtractTools/CB_NEWREG_BIOUPDT_POOL_HOURLY/extract_CB_NEWREG_BIOUPDT_POOL_HOURLY_hours.sh

sleep 15m

beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.CB_NEWREG_BIOUPDT_POOL_HOURLY;"

BASE_LOCATION=/nas/share05/tools/DBExtrct
KAFKA_SECURITY="-Djava.security.auth.login.config=/export/home/daasuser/Kamanja/config/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
java -Dlog4j.configurationFile=${BASE_LOCATION}/conf/log4j2.xml \
${KAFKA_SECURITY} \
-cp ${BASE_LOCATION}/lib/ExtDependencyLibs_2.11.jar:${BASE_LOCATION}/lib/ExtDependencyLibs2_2.11.jar:${BASE_LOCATION}/lib/KamanjaInternalDeps_2.11.jar:${BASE_LOCATION}/lib/dbextract_2.11.jar:${BASE_LOCATION}/lib/dbutils_2.11.jar:${BASE_LOCATION}/lib/kafka-clients.jar:${BASE_LOCATION}/lib/presto-jdbc.jar  com.ligadata.utils.dbextract.DBExtract  -cfg  ${BASE_LOCATION}/conf/conf.json -s liveearth_simreg -sd $Start_Date -ed $End_Date -sh $Start_Hr -eh $End_Hr -p 2 -cn  presto1

   fi
