package AlertProducer

import net.liftweb.json._
object ConfigParser {
  var hdfs_defaultName = ""
  var hdfs_auditPath = ""
  var kafka_hostList = ""
  var kafka_topicName = ""
  var csvPath = ""
   def getConfig(configPath: String): net.liftweb.json.JValue = try {
       val cfg = parse(scala.io.Source.fromFile(configPath).getLines.mkString)
       println("JSON config file has been parsed successfully")
       cfg
     } catch {
       case e: Exception => {
         println("Error while parsing the JSON config file"+e.getMessage)
         System.exit(1)
         null
       }
     }

  def parseConfig(configObj: net.liftweb.json.JValue): Unit = {
    val hdfsConfig = (configObj \\ "hdfsConfig").children(0).values.asInstanceOf[Map[String, String]]
    println("hdfsConfig: " + hdfsConfig)
    hdfs_defaultName = hdfsConfig.getOrElse("defaultName", "")
    hdfs_auditPath = hdfsConfig.getOrElse("auditPath", "")

    val kafkaConfig = (configObj \\ "kafkaConfig").children(0).values.asInstanceOf[Map[String, String]]
    println("kafkaConfig: " + kafkaConfig)
    kafka_hostList = kafkaConfig.getOrElse("hostList", "")
    kafka_topicName = kafkaConfig.getOrElse("topicName", "")

    csvPath = (configObj).values.asInstanceOf[Map[String, String]].getOrElse("csvPath", "")
    println("hdfs_defaultName: " + hdfs_defaultName)
    println("hdfs_auditPath: " + hdfs_auditPath)
    println("csvPath: " + csvPath)
    println("kafka_hostList: " + kafka_hostList)
    println("kafka_topicName: " + kafka_topicName)

  }
}
