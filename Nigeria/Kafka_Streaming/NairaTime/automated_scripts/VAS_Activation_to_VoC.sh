#!bin/bash

Start_Date=$(date '+%Y%m%d')
End_Date=$(date '+%Y%m%d')
Start_Hr=$(date -d '-1 hour' '+%H')
End_Hr=$(date -d '-0 hour' '+%H')

BASE_LOCATION=/nas/share05/tools/DBExtrct
KAFKA_SECURITY="-Djava.security.auth.login.config=/home/daasuser/Kamanja/config/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
java -Dlog4j.configurationFile=${BASE_LOCATION}/conf/log4j2.xml \
${KAFKA_SECURITY} \
-cp ${BASE_LOCATION}/lib/ExtDependencyLibs_2.11.jar:${BASE_LOCATION}/lib/ExtDependencyLibs2_2.11.jar:${BASE_LOCATION}/lib/KamanjaInternalDeps_2.11.jar:${BASE_LOCATION}/lib/dbextract_2.11.jar:${BASE_LOCATION}/lib/dbutils_2.11.jar:${BASE_LOCATION}/lib/kafka-clients.jar:${BASE_LOCATION}/lib/presto-jdbc.jar  com.ligadata.utils.dbextract.DBExtract  -cfg  ${BASE_LOCATION}/conf/conf.json -s vas_activation_to_voc -sd $Start_Date -ed $End_Date -sh $Start_Hr -eh $End_Hr -p 2 -cn  presto1
