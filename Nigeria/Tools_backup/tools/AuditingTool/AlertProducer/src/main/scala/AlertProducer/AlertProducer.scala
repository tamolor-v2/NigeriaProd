package AlertProducer

object AlertProducer extends App {
  // Initiate variable from commandLine
  var configFile = ""
  var message = ""

  // Initiate variable from configFile
  var kafka_hostList = ""
  var kafka_topicName = ""
  var hdfs_defaultName = ""
  var hdfs_auditPath = ""
  var csvPath = ""

  // parse args from command line
  ArgsParser.parsArgs(args)
  configFile = ArgsParser.configFile
  message = ArgsParser.message

  // parse json config
  val configObj = ConfigParser.getConfig(configFile)
  ConfigParser.parseConfig(configObj)

  // get values from config
  kafka_hostList = ConfigParser.kafka_hostList
  kafka_topicName = ConfigParser.kafka_topicName
  hdfs_defaultName = ConfigParser.hdfs_defaultName
  hdfs_auditPath = ConfigParser.hdfs_auditPath
  csvPath = ConfigParser.csvPath

  // Pass the message to kafka
  KakfaUtils.passMessageToTopic(kafka_hostList,kafka_topicName,message)

  // Write the message to csv on local and hdfs
  FileUtils.WriteToFile(message,csvPath,hdfs_auditPath,hdfs_defaultName)

  println("Done")

}
