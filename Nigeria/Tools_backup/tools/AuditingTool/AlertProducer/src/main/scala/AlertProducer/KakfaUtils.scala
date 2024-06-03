package AlertProducer



import java.util.Properties
import java.util.function
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.clients.CommonClientConfigs



object KakfaUtils {

    def InitProducer(hostList: String): KafkaProducer[String,String] ={

      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("java.security.auth.login.config", "/etc/kafka/2.6.1.0-129/0/kafka_jaas.conf")
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
      System.setProperty("sun.security.krb5.debug", "false")

      val  props = new Properties()
      props.put("bootstrap.servers", hostList)
      props.put("sasl.kerberos.service.name", "kafka")
      props.put("sasl.kerberos.service.name.doc","kafka/datanode01038.mtn.com@MTN.COM")
      props.put("sasl.kerberos.kinit.cmd","/usr/bin/kinit")
      props.put("sasl.kerberos.kinit.cmd.doc","/etc/security/keytabs/kafka.service.keytab")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("security.inter.broker.protocol", "SASL_PLAINTEXT")
      props.put("security.protocol", "SASL_PLAINTEXT")
      props.put("security.mechanism", "GSSAPI")

      props.put(ProducerConfig.ACKS_CONFIG, "all")


      val producer = new KafkaProducer[String, String](props)
      producer
    }
  val TOPIC = ConfigParser.kafka_topicName
  def passMessageToTopic(hostList: String,topicName: String, message: String): Unit ={
        val producer = InitProducer(hostList)
        val record = new ProducerRecord(TOPIC, "key", message)
        producer.send(record)
        producer.close()
      }
}
