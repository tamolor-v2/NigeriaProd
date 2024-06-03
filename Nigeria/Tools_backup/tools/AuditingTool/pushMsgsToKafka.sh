
dt=$(date -d "$date - 1 day" +%Y%m%d)

startTime=$(date +"%Y-%m-%d %H:%M:%S.%3N")
endTime=$(date +"%Y-%m-%d %H:%M:%S.%3N")

detailedMsg="The details needed!"
splitChar=$(printf '\001')
stats="Success"
reportName="HUAWEI_CEM_PROTOCOL_DATA"
kinit="yes"

msg="${i}${splitChar}${reportName}${splitChar}${startTime}${splitChar}${endTime}${splitChar}${stats}${splitChar}${detailedMsg}${splitChar}${dt}"
if [ $kinit == "yes" ]  
 then
  kinit -kt /etc/security/keytabs/kafka.service.keytab kafka/datanode01038.mtn.com@MTN.COM
  java -Dsun.security.krb5.debug=true -Djava.security.auth.login.config=/etc/kafka/2.6.1.0-129/0/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -DjavasplitChar.security.auth.useSubjectCredsOnly=false  -cp /mnt/beegfs_api/Nabil/FacebookProject/lib/AlertProducer-assembly-0.1.jar AlertProducer.AlertProducer --configFile /mnt/beegfs_api/Nabil/FacebookProject/conf/AlertProducer.json --message "$msg"
else
  java -Dsun.security.krb5.debug=true -Djava.security.auth.login.config=/etc/kafka/2.6.1.0-129/0/kafka_jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf -DjavasplitChar.security.auth.useSubjectCredsOnly=false  -cp /mnt/beegfs_api/Nabil/FacebookProject/lib/AlertProducer-assembly-0.1_withoutK.jar AlertProducer.AlertProducer --configFile /mnt/beegfs_api/Nabil/FacebookProject/conf/AlertProducer.json --message "$msg"
fi  
