Start_Date=$(date -d '-1 day' +\%Y\%m\%d)
End_Date=$(date -d '-1 day' +\%Y\%m\%d)

BASE_LOCATION=/nas/share05/tools/DBExtrct
KAFKA_SECURITY="-Djava.security.auth.login.config=/export/home/daasuser/Kamanja/config/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
#KAFKA_SECURITY="-Djava.security.auth.login.config=/home/daasuser/Kamanja/config/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf"
java -Dlog4j.configurationFile=${BASE_LOCATION}/conf/log4j2.xml \
${KAFKA_SECURITY} \
-cp ${BASE_LOCATION}/lib/ExtDependencyLibs_2.11.jar:${BASE_LOCATION}/lib/ExtDependencyLibs2_2.11.jar:${BASE_LOCATION}/lib/KamanjaInternalDeps_2.11.jar:${BASE_LOCATION}/lib/dbextract_2.11.jar:${BASE_LOCATION}/lib/dbutils_2.11.jar:${BASE_LOCATION}/lib/kafka-clients.jar:${BASE_LOCATION}/lib/presto-jdbc.jar  com.ligadata.utils.dbextract.DBExtract  -cfg  ${BASE_LOCATION}/conf/conf.json -s sms_cdr_transaction -sd $Start_Date -ed $End_Date -p 2 -cn  presto1
