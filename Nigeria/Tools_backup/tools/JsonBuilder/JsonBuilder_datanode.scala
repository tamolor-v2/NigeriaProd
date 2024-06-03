#!/bin/sh
exec scala -Djava.security.auth.login.config=/home/daasuser/Kamanja/config/kafka_jaas.conf -cp /mnt/beegfs/tools/JsonBuilder/ExtDependencyLibs_2.11-1.5.3.jar:/mnt/beegfs/tools/JsonBuilder/kafka-clients-0.10.2.1.jar -savecompiled "$0" "$@"
!#
// /usr/hdp/current/kafka-broker/config/kafka_jaas.conf
// /home/daasuser/kafka_client_jaas.conf
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by Yousef on 10/27/2018.
  */
object JsonBuilder {

  case class JobInfo(id: Int, runTime: String, status: Int, logDirectory: String, hostName: String, errorMessage: String, errorMessageDescription: String, step: Int, generalMessage: String, runID: Int)

  var id: Int  = -1
  var run_time: String = ""
  var status: Int = -2
  var log_directory: String = ""
  var hostname: String = ""
  var error_message: String = ""
  var error_message_description: String = ""
  var step: Int = 1
  var general_message = ""
  var helpString = ""
  var brokers: String = "datanode01001.mtn.com:6667,datanode01002.mtn.com:6667,datanode01003.mtn.com:6667,datanode01004.mtn.com:6667,datanode01005.mtn.com:6667,datanode01006.mtn.com:6667,datanode01007.mtn.com:6667,datanode01008.mtn.com:6667,datanode01009.mtn.com:6667,datanode01010.mtn.com:6667,datanode01011.mtn.com:6667,datanode01012.mtn.com:6667,datanode01013.mtn.com:6667,datanode01014.mtn.com:6667,datanode01015.mtn.com:6667,datanode01016.mtn.com:6667,datanode01017.mtn.com:6667,datanode01018.mtn.com:6667,datanode01019.mtn.com:6667,datanode01020.mtn.com:6667,datanode01021.mtn.com:6667,datanode01022.mtn.com:6667,datanode01023.mtn.com:6667,datanode01024.mtn.com:6667,datanode01025.mtn.com:6667,datanode01026.mtn.com:6667,datanode01027.mtn.com:6667,datanode01028.mtn.com:6667,datanode01029.mtn.com:6667,datanode01030.mtn.com:6667,datanode01031.mtn.com:6667,datanode01032.mtn.com:6667,datanode01033.mtn.com:6667,datanode01034.mtn.com:6667,datanode01035.mtn.com:6667,datanode01036.mtn.com:6667,datanode01037.mtn.com:6667,datanode01038.mtn.com:6667,datanode01039.mtn.com:6667,datanode01040.mtn.com:6667,datanode01041.mtn.com:6667,datanode01042.mtn.com:6667"
  var topic: String = ""
  var message: String = ""
  val runTimeDateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val runIdDateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

  def getDateTime(option: String): String ={
    val submittedDateConvert = new Date()
    val submittedDateTime =
      if(option.equals("runTime"))
        runTimeDateFormatter.format(submittedDateConvert)
      else
        runIdDateFormatter.format(submittedDateConvert)
    submittedDateTime
  }

  def parsArgs = (args: Array[String]) => {
    args.sliding(2, 2).toList.collect {
      case Array("--id", argId: String) => id = argId.toInt
      case Array("--status", argStatus: String) => status = argStatus.toInt
      case Array("--log_directory", argLogDirectory: String) => log_directory = argLogDirectory
      case Array("--hostname", argHostName: String) => hostname = argHostName
      case Array("--error_message", argErrorMessage: String) =>  error_message = argErrorMessage.trim.toLowerCase
      case Array("--error_message_description", argErrorMessageDescription) =>  error_message_description = argErrorMessageDescription
      case Array("--step", argStep) =>  step = argStep.toInt
      case Array("--general_message", argGeneralMessage: String) =>  general_message = argGeneralMessage
      case Array("--brokers", argBrokers: String) =>  brokers = argBrokers      
      case Array("--topic", argtopic: String) =>  topic = argtopic
      case Array("--help") | _ => helpString = "--id  --status --log_directory  --hostname --error_message --error_message_description --step --general_message --help"
        System.exit(0)
    }
  }

  def validateArgs(): Unit ={
    if (id == -1 ) {
      println("Error: id = -1")
      System.exit(1)
    }

    if (status == -2) {
      println("Error: status = -2")
      System.exit(1)
    }

    if (hostname == null || hostname.length == 0) {
      println("Error: hostname is null")
      System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    parsArgs(args)
    validateArgs
    val jobDesc = JobInfo(id, getDateTime("runTime"), status, log_directory, hostname, error_message, error_message_description, step, general_message, getDateTime("runID").toInt)
    //println(write(jobDesc))
    message = write(jobDesc)

    sendMessages()
  }

  private def configuration : Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put("security.protocol","SASL_PLAINTEXT")
    props.put("sasl.kerberos.service.name","kafka")
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props
  }

  val producer = new KafkaProducer[String, String](configuration)

  def sendMessages(): Unit = {
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
    producer.close()
    }
}
