#!/bin/sh
exec scala -cp /nas/share05/tools/presto-jdbc-0.191.jar:/usr/hdp/current/hadoop-client/conf:/home/daasuser/FlareCluster/Flare/lib/system/ExtDependencyLibs_2.11-1.7.0.jar:/home/daasuser/FlareCluster/Flare/lib/system/ExtDependencyLibs2_2.11-1.7.0.jar -savecompiled "$0" "$@"
!#
import java.io.{File, _}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.sys.process.{ProcessLogger, _}
import scala.util.matching.Regex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileUtil, LocalFileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.authentication.util.KerberosUtil
object DeduplicationScript {
  var configFile: String = ""
  var feedsMetadata: String = ""
  case class Feed(seq: Int, md5_compute: String)
  var schema_name = ""
  var dates = Array[Int]()
  // var dryRun = false
  var dedupFeed = ""
  var temp_table_name = ""
  var HadoopFolder = ""
  val CreateTempTable =
    """create table {schema_name}.{table_name_tmp} as select *, {md5_compute} as md5_key, row_number() over( partition by {md5_compute} order by file_name, kamanja_loaded_date desc) as dedup_rank from {schema_name}.{table_name} where tbl_dt= {tbl_dt}"""
  val insertTempTable =
    """insert into {schema_name}.{table_name_tmp}  select *, {md5_compute} as md5_key, row_number() over( partition by {md5_compute} order by file_name, kamanja_loaded_date desc) as dedup_rank from {schema_name}.{table_name} where tbl_dt= {tbl_dt}"""
  val OriginPartitionCount =
    "select count(*) from {schema_name}.{table_name} where tbl_dt= {tbl_dt} "
  val TempPartitionCount =
    "select count(*) from {schema_name}.{table_name_tmp} where tbl_dt= {tbl_dt} and dedup_rank = 1"
  val DropOriginalPartition =
    "ALTER TABLE {schema_name}.{table_name} DROP IF EXISTS PARTITION ({partition_field})"
  val CountDuplicate = "select coalesce(sum(cnt), 0) as cnt from (select {md5_compute},(count(*)-1) cnt  from {schema_name}.{table_name} where " +
    "tbl_dt={tbl_dt} group by {md5_compute} having count(*)>1)"
  val hivecountDuplicate = "select coalesce(sum(cnt), 0) as cnt from (select {md5_compute},(count(*)-1) cnt  from {schema_name}.{table_name} where " +
    "tbl_dt={tbl_dt} group by {md5_compute} having count(*)>1) a "
  val OriginalPartitionOverWrite =
    "insert into {schema_name}.{table_name}  select {field_List} from {schema_name}.{table_name_tmp} where tbl_dt={tbl_dt} and dedup_rank = 1"
  val DupPartitionOverWrite =
    "insert into {schema_name}.dupdata select md5_key, cast(dedup_rank as int), file_name, file_offset, kamanja_loaded_date, file_mod_date, date_key, msisdn_key, original_timestamp_enrich, localtimestamp as deduptime, '{table_name}' as feedtype, tbl_dt from {schema_name}.{table_name_tmp} where tbl_dt={tbl_dt} and dedup_rank > 1"
  var fieldList = ""
  val jDBCManager = new JDBCManager(
    "jdbc:presto://10.1.197.145:8999/hive5/flare_8",
     // "jdbc:presto://10.1.197.158:8844/hive5/flare_8",
    "daasuser",
    null)

  def main(args: Array[String]): Unit = {
    Log.logMsg("Started")
    //    parsArgs(Array("--configFile", "C:/Testing/dedup/dedup.conf","--date","20180927","--feed","MSC_CDR"))
    parsArgs(args)
    if (dates.isEmpty) Log.logError("No date is supplied", true)
    Seq("kinit",
      "-k",
      "-t",
      "/etc/security/keytabs/daasuser.keytab",
      "daasuser@MTN.COM") !;
    Log.logMsg("Successfully Parsed args")
    val feedsList = parseMetadata(configFile)
    println(feedsList)
    dates.foreach(date => {
      runForDay(date, feedsList)
    })
  }


  def runForDay(date: Int, feedsList: Map[String, Feed]): Unit = {
    Log.logMsg("Started for day:" + date)
//    parsArgs(Array("--configFile", "C:/Testing/dedup/dedup.conf","--date","20180927","--feed","MSC_CDR"))
    if (date == 0) Log.logError("No date is supplied", true)
    Seq("kinit",
        "-k",
        "-t",
        "/etc/security/keytabs/daasuser.keytab",
        "daasuser@MTN.COM") !;
    val regex = new Regex("\"(.*?)\"|([^\\s]+)")
    val (feed, data) = getFeedInfo(feedsList)
    if (feed == "" || data.md5_compute.trim.length == 0)
      Log.logError("Feed couldn't be found in config file", true)
    else Log.logMsg(feed + " Feed Metadata found")
    temp_table_name = feed + "_tmp_" + date.toString
    val tmpTableStartExists =
      jDBCManager.tableExists(schema_name, temp_table_name)
    if (tmpTableStartExists) {
      val tmpCountForStart = getCount(feed, TempPartitionCount, data, date)
      Log.logMsg("Count for Temp table= " + tmpCountForStart)

      if (tmpCountForStart > 0) {
        Log.logMsg(
          "Temp table already has data, the script will terminate. please contact Ops technical lead for further instructions")
        System.exit(1)
      }
    }

    val columns = extractColumnsNames(schema_name + "." + feed)
    temp_table_name = feed + "_tmp_" + date.toString
    fieldList = columns.mkString(",")
    val duplicateCount = getCount(feed, CountDuplicate, data, date)
    if (duplicateCount > 0) {
      Log.logMsg("Count of duplicates = " + duplicateCount)
      val originalCount = getCount(feed, OriginPartitionCount, data, date)
      Log.logMsg("Count of Original Partition = " + originalCount)
      val filesToDelete = getCurrentHDFSFiles(feed, date)
      if (filesToDelete.size == 1 && filesToDelete.head == "ERROR") {
        Log.logMsg("ERROR getting list of hdfs files... exitting")
        System.exit(0)
      }
      Log.logMsg("Creating temp table")
      val createTemp =
        createTempTable(feed, CreateTempTable, insertTempTable, data, date)
      val tmpCount = getCount(feed, TempPartitionCount, data, date)
      Log.logMsg("Count for Temp table= " + tmpCount)
      if (originalCount == (tmpCount + duplicateCount)) {

        Log.logMsg(
          "The total for (tmpCount+duplicateCount) = OriginalPartitionCount: " + (tmpCount + duplicateCount) + "=" + originalCount)
        var (exitCode, stdout, stderr) = deletePartitionFiles(filesToDelete)
        println(exitCode + ":" + stdout + ":" + stderr)
        if (exitCode == 0) {

          Log.logMsg("Old data moved to trash at: " + stderr)
          populatePartition(feed, OriginalPartitionOverWrite, date)
          populatePartition(feed, DupPartitionOverWrite, date)

        } else {
          Log.logMsg("Old data wasn't deleted... Exitting")
          System.exit(0)
        }
      } else {
        Log.logMsg(
          "The total for (tmpCount+duplicateCount) <> OriginalPartitionCount: " + (tmpCount + duplicateCount) + "<>" + originalCount)
      }
    } else
      Log.logMsg("No Duplication found for " + feed + " in partition:" + date)
    Log.logMsg("Finished")
  }

  class HdfsConfig {
    var hostsList: Array[String] = Array.empty[String]
    var principal: String = _
    var keytab: String = _
    var hadoopConfig: List[(String, String)] = null
    var connectionTimeout: Int = 15000
  }

  def getConfig(
      conf: HdfsConfig): (org.apache.hadoop.conf.Configuration,
                          org.apache.hadoop.security.UserGroupInformation) = {
    var ugi: org.apache.hadoop.security.UserGroupInformation = null
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()
    if (conf.hostsList != null && conf.hostsList.length > 0) {
      val hosts = conf.hostsList
        .map(h => if (h != null) h.trim else null)
        .filter(h => h != null && h.length > 0)
      if (hosts.length > 0)
        hdfsConfig.set("fs.default.name", conf.hostsList.mkString(","))
    }
    hdfsConfig.set(
      "fs.hdfs.impl",
      classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hdfsConfig.set("fs.file.impl",
                   classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    try {

      if (conf.hadoopConfig != null && !conf.hadoopConfig.isEmpty) {
        conf.hadoopConfig.foreach(conf => {
          hdfsConfig.set(conf._1, conf._2)
        })
      }

      //hdfsConfig.set("hadoop.job.ugi", "hadoop");//user ???
      if (conf.principal != null && conf.keytab != null && !conf.principal.trim.isEmpty && !conf.keytab.trim.isEmpty) {
        hdfsConfig.set("hadoop.security.authentication", "Kerberos")
        //ugi = KerberosUtils.KamanjaloginUsrFromKeytabAndReturnUGI(hdfsConfig, conf.principal.trim, conf.keytab.trim)
        org.apache.hadoop.security.UserGroupInformation
          .setConfiguration(hdfsConfig)
        org.apache.hadoop.security.UserGroupInformation
          .loginUserFromKeytab(conf.principal.trim, conf.keytab.trim)
        ugi = org.apache.hadoop.security.UserGroupInformation.getLoginUser()
      }
    } catch {
      case e: Exception => {
        //Log.error("Failed to createConfig.", e)
        throw e
      }
    }
    (hdfsConfig, ugi)
  }

  def deletePartitionFiles(fileList: List[String]): (Int, String, String) = {
    Log.logMsg("Started deleting hdfs files ")
    val hdfsConf = new HdfsConfig

    hdfsConf.hostsList = Array("hdfs://ngdaas")
    hdfsConf.principal = "daasuser@MTN.COM"
    hdfsConf.keytab = "/etc/security/keytabs/daasuser.keytab"
    hdfsConf.hadoopConfig = List[(String, String)](
      ("dfs.nameservices", "ngdaas"),
      ("dfs.ha.namenodes.ngdaas", "nn1,nn2"),
      ("dfs.namenode.rpc-address.ngdaas.nn1", "master01001.mtn.com:8020"),
      ("dfs.namenode.rpc-address.ngdaas.nn2", "master01002.mtn.com:8020"),
      ("dfs.client.failover.proxy.provider.ngdaas",
       "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    )

    val (hdfsConf1, ugi) = getConfig(hdfsConf)
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsConf1);
    val delRecursive = false
//BUGBUG:: Not handled any exception here
    fileList.par.foreach(file => {
      fs.delete(new org.apache.hadoop.fs.Path(file), delRecursive)
    })
    Log.logMsg("finished deleting hdfs files ")

    (0, "", "")
  }

  def populatePartition(feed: String, sqlQuery: String, date: Int) = {
    val query = sqlQuery
      .replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("{table_name_tmp}", temp_table_name)
      .replace("""{tbl_dt}""", date.toString)
      .replace("""{field_List}""", fieldList)
    Log.logMsg("Executing Query: " + query)
    jDBCManager.executeQueryOnce(query, feed)
  }
  def getCurrentHDFSFiles(feed: String, date: Int): List[String] = {
    var (exitValue, stdoutStream, stderrStream) = runCommand(
      Seq("hadoop",
          "fs",
          "-ls",
          HadoopFolder + "/" + feed + "/tbl_dt=" + date + "/*"))
//#, "|", "awk","-F","' '","'{print $8}'"))
//#println("exitValue"+exitValue)
//#println("stdoutStream"+stdoutStream)
//#println("stderrStream"+stderrStream)
//val finalFiles=stdoutStream.split("")
    if (exitValue == 0)
      stdoutStream
        .split("\n")
        .toList
        .map(a => a.split("\\s+")(7))
        .filterNot(a => a.contains("""/."""))
    else
      List("ERROR")
  }
  /*def deletePartitionFiles(fileList:List[String]):(Int,String,String)={
//println("hadoop fs -rm "+HadoopFolder+"/"+feed+"/tbl_dt="+date+"/D*")
Log.logMsg("Deleting hdfs files List")
val finalDeleteList=fileList.mkString(" ")
//println("hadoop fs -rm "+finalDeleteList)
var xV=1
var sOS=""
var sES=""
for(file<-fileList){
var (exitValue, stdoutStream, stderrStream) = runCommand(Seq("hadoop","fs","-rm",file))
println(exitValue+stdoutStream+"--->"+stderrStream)
xV=exitValue
sOS=stdoutStream
sES=stderrStream
}
(xV,sOS,sES)
}
   */
  def extractColumnsNames(feed_name: String) = {
    var (exitValue, stdoutStream, stderrStream) = runCommand(
      Seq("/opt/presto/bin/presto",
          "--server",
          "10.1.197.145:8999",
          "--catalog",
          "hive5",
          "--execute",
          "show columns from " + feed_name))
    val columns = stdoutStream
      .split("\n")
      .map(a => a.split(",")(0).replace("\"", ""))
      .toList
    columns

//    val columns=show.split("\n").map(a=>a.split(",")(0).replace("\"","")).mkString(",")
//    columns
  }

  def getFeedInfo(feedList: Map[String, Feed]): (String, Feed) = {
    val feed = feedList.getOrElse(dedupFeed, new Feed(0, ""))
    println(feed + " found")
    return (dedupFeed, feed)
  }

  def createTempTable(feed: String,
                      sqlCreateQuery: String,
                      sqlInsertQuery: String,
                      data: Feed, date: Int) = {
    var query = ""
    val tmpTableExists =
      jDBCManager.prepareTempTable(schema_name, temp_table_name)
    //val tmpTableExists=true
    if (tmpTableExists == false) {
      query = sqlCreateQuery
        .replace("""{schema_name}""", schema_name)
        .replace("""{table_name}""", feed)
        .replace("{table_name_tmp}", temp_table_name)
        .replace("""{tbl_dt}""", date.toString)
        .replace("""{field_List}""", fieldList)
        .replace("{md5_compute}", data.md5_compute)
    } else {
      query = sqlInsertQuery
        .replace("""{schema_name}""", schema_name)
        .replace("""{table_name}""", feed)
        .replace("{table_name_tmp}", temp_table_name)
        .replace("""{tbl_dt}""", date.toString)
        .replace("""{field_List}""", fieldList)
        .replace("{md5_compute}", data.md5_compute)
    }
    Log.logMsg("Executing Query: " + query)

    jDBCManager.executeQueryOnce(query, temp_table_name)
  }
  def getCount(feed: String, sqlQuery: String, data: Feed, date: Int): Long = {
    var finalCount = 0L
    val query = sqlQuery
      .replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("{table_name_tmp}", temp_table_name)
      .replace("{md5_compute}", data.md5_compute
      )
      .replace("""{tbl_dt}""", date.toString)
    Log.logMsg("Executing 111 query: " + query)
    val count = jDBCManager.executeSelectQuery(query)
    // println(count)
    try {
      val qCount = if (count != None) count.toLong else 0L
      finalCount = qCount.asInstanceOf[Long]
      return finalCount
    } catch {
      case ex: Throwable => {
        Log.logMsg("Eror: Count=0L" + ex.printStackTrace().toString)
        return 0L
      }
    }
  }
  def runCommand(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue =
      cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }

  def parsArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--configFile", argDataDate: String) =>
          configFile = argDataDate
        case Array("--feed", argDataDate: String) => dedupFeed = argDataDate
        case Array("--dates", argDataDates: String) => dates = argDataDates.split(",").map(s => s.trim).filter(s => s.nonEmpty).map(s => s.toInt)
        // case Array("--dryrun", argVal: String) => dryRun = argVal.trim.toLowerCase.startsWith("y")
      } else
      Log.logError("No config file is supplied", true)
    if (dedupFeed.trim == "")
      Log.logError("Please provide a feed to clean", true)
  }

  def parseMetadata(path: String): Map[String, Feed] = {
    var FeedMetadata = new ListBuffer[Feed]()
    var FeedMetadataMap = Map[String, Feed]()
    val config =
      ConfigFactory.parseFile(new File(path)).getConfig("Dedups.LoadCluster")
    val clusterName = config.getString("ClusterName")
    schema_name = config.getString("Schema")
    HadoopFolder = config.getString("HadoopFolder")

    val allFeeds = config.getConfigList("Feeds")

    allFeeds.foreach(feed => {
      val md5_compute = if (feed.hasPath("md5_compute")) feed.getString("md5_compute") else ""
      val seq = if (feed.hasPath("seq")) feed.getInt("seq") else 0
      val feedName = if (feed.hasPath("feed")) feed.getString("feed") else ""
      val feedObj = Feed(seq, md5_compute)
      FeedMetadata += feedObj
      FeedMetadataMap += ((feedName, feedObj))
    })

    FeedMetadataMap
  }

  /*
  def processfeeds(metadata: String): Feed = {
    val result = metadata.split("=").map(a => a.trim)
    var seq = 0
    var feed_name = ""
    var reg = ""
    var md5_compute = ""
    var ops = ""
    var hdfsCheck = ""
    var rg = ""
    var feeds_list = ListBuffer[String]()
    var minitar_filename = ""
    for (a <- result) {
      val kv = a.split("=")
      if (kv(0).toLowerCase == "seq")
        seq = kv(1).toInt
      else if (kv(0).toLowerCase == "md5_compute")
        md5_compute = kv(1)
    }
    return Feed(seq, md5_compute)
  }
  */
}

class JDBCManager(private val url: String,
                  private val user: String,
                  private val pass: String) {
  private val conn: Connection =
    DriverManager.getConnection(this.url, this.user, this.pass)
  def getCountDup(query: String): Long = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    if (conn != null && !conn.isClosed) {
      Log.logMsg("Executing count statement: " + query)
      statement = conn.createStatement()
      val result = statement.executeQuery(query)
      val cnt = if (result.next) result.getString(1).toLong else 0L
      return cnt
    }
    return 0L
  }
  private val maxRetry = 1
  def executeSelectQueryOnce(query: String): String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    if (conn != null && !conn.isClosed) {
      Log.logMsg("generating table statement ")
      statement = conn.createStatement()
      Log.logMsg("executing statement")
      val result = statement.executeQuery(query)
      val cnt = if (result.next) result.getString(1) else ""
      return cnt
    }
    return ""
  }

  def executeSelectQuery(query: String): String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    while (retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("generating table statement ")
          statement = conn.createStatement()
          Log.logMsg("executing statement")
          val result = statement.executeQuery(query)
          val cnt = if (result.next) result.getString(1) else ""
          return cnt
        }
      } catch {
        case ex: Throwable => {
          Log.logMsg("reprocessing ")
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          ex.printStackTrace
        }
      }
    }
    return ""
    //(resultSet, statement)
  }
  def prepareTempTable(schema_name: String, table_name: String): Boolean = {
    val query = "show tables from " + schema_name + """ like '%""" + table_name.toLowerCase + """%'"""
    Log.logMsg("Executing query: " + query)
    var statement: Statement = null
    var resultSet: ResultSet = null
//  var result=1
    var status = "Start"
    Log.logMsg("Checking if temp table exists")

    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("Checking if temp table exists")
        statement = conn.createStatement()
        val result = statement.executeQuery(query)

        val tableStatus = if (result.next()) result.getString(1) else ""
        if (tableStatus.trim.length > 0) {
          Log.logMsg("Table exists, deleting Data")
          Log.logMsg("delete from " + schema_name + "." + table_name)
          statement.executeUpdate(
            "delete from " + schema_name + "." + table_name)
          return true
        } else
          return false
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("reprocessing " + table_name)
        Log.logMsg("Exception while executing query: %s ".format(query))
        Log.logMsg(
          "ERROR - ERROR ----------->" + table_name + ": " + ex.getMessage + ex.printStackTrace.toString)
      }
    }
    false
  }
  def tableExists(schema_name: String, table_name: String): Boolean = {
    val query = "show tables from " + schema_name + """ like '%""" + table_name.toLowerCase + """%'"""
    Log.logMsg("Executing query: " + query)
    var statement: Statement = null
    var resultSet: ResultSet = null
//  var result=1
    var status = "Start"
    Log.logMsg("Checking if temp table exists")

    try {
      if (conn != null && !conn.isClosed) {
        statement = conn.createStatement()
        val result = statement.executeQuery(query)

        val tableStatus = if (result.next()) result.getString(1) else ""
        if (tableStatus.trim.length > 0) {
          return true
        } else
          return false
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("reprocessing " + table_name)
        Log.logMsg("Exception while executing query: %s ".format(query))
        Log.logMsg("----------->" + table_name + ": " + ex.getMessage)
      }
    }
    false
  }

  def executeQueryOnce(query: String, table_name: String): String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    var result = 1
    var status = "Start"
    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("generating table statement for: " + table_name)
        statement = conn.createStatement()
        Log.logMsg("executing statement :" + table_name)
        result = statement.executeUpdate(query)
        Log.logMsg("successfully populated" + table_name)
        return "Success"
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("reprocessing " + table_name)
        Log.logMsg("Exception while executing query: %s ".format(query))
        Log.logMsg("----------->" + table_name + ": " + ex.getMessage)
      }
    }
    status = "Failed"
    status
  }
  def executeFinalQuery(query: String): String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    var result = 1
    var status = "Start"
    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("generating table statement for final query\r\n" + query)
        statement = conn.createStatement()
        result = statement.executeUpdate(query)
        return "Success"
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("Exception while executing query: %s ".format(query))
      }
    }
    status = "Failed"
    status
  }

  def executeSelectQuery(query: String, table_name: String): String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    while (retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("generating table statement for: " + table_name)
          statement = conn.createStatement()
          Log.logMsg("executing statementfor :" + table_name)
          val result = statement.executeQuery(query)
          Log.logMsg("successfully executed statement for " + table_name)
          val rs = if (result.next) result.getString(1) else ""
          return rs
        }
      } catch {
        case ex: Throwable => {
          Log.logMsg("reprocessing " + table_name)
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          ex.printStackTrace
        }
      }
    }
    return ""
    //(resultSet, statement)
  }

  def executeQuery(query: String): String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    var result = 1
    var status = "Start"
    while (retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("generating table statement ")
          statement = conn.createStatement()
          Log.logMsg("executing statement")
          result = statement.executeUpdate(query)
          return "success"
        }
      } catch {
        case ex: Throwable => {
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          //          Log.logMsg(ex.getStackTrace.toList.foreach(println))
          ex.printStackTrace
        }
      }
    }
    //(resultSet, statement)
    status = "failed"
    status
  }
}
object Log {

  val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def logMsg(msg: String): Unit = {
    val time = LocalDateTime.now().format(formatter)
    println(time + " - " + msg)
  }

  def logError(msg: String,
               exit: Boolean,
               throwable: Throwable = null): Unit = {
    val time = LocalDateTime.now().format(formatter)
    System.err.println(time + " - %s".format(msg))
    if (throwable != null) {
      System.err.println(getsearchString(throwable))
    }
    if (exit) {
      System.exit(-1)
    }
  }

  private def getsearchString(throwable: Throwable): String = {
    val sw = new StringWriter
    throwable.printStackTrace(new PrintWriter(sw))
    val exceptionAsString = sw.toString
    sw.close
    exceptionAsString
  }
}
object Implicits {
  implicit class CaseClassToMap(c: AnyRef) {
    def toMapWithFields: Map[String, Any] = {
      val fields = (Map[String, Any]() /: c.getClass.getDeclaredFields) {
        (a, f) =>
          f.setAccessible(true)
          a + (f.getName -> f.get(c))
      }

      //s"${c.getClass.getName}(${fields.mkString(", ")})"
      fields
    }
  }
}
DeduplicationScript.main(args)
