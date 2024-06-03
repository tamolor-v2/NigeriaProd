#!/bin/sh
exec scala -cp /home/daasuser/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

import java.io.{File, _}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import sun.rmi.runtime.Log

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.sys.process.{ProcessLogger, _}
import scala.util.matching.Regex
object Deduplicate {
  var configFile: String =""
  var feedsMetadata:String=""
  case class Feed(seq:Int,UniqueFields:String)
  var schema_name = ""
  var date=0
  var dedupFeed=""
  val CreateTempTable=   """create table {schema_name}.{table_name_tmp} as select {field_List} from( select *,row_number() over( partition by {unique_fields}) as rank from {schema_name}.{table_name} where tbl_dt= {tbl_dt}) c where rank=1"""
  val OriginPartitionCount="select count(*) from {schema_name}.{table_name} where tbl_dt= {tbl_dt} "
  val TempPartitionCount="select count(*) from {schema_name}.{table_name_tmp} where tbl_dt= {tbl_dt} "
  val DropOriginalPartition="ALTER TABLE {schema_name}.{table_name} DROP IF EXISTS PARTITION ({partition_field})"
  val CountDuplicate="select sum(cnt) from (select {unique_fields},(count(*)-1) cnt  from {schema_name}.{table_name} where " +
    "tbl_dt={tbl_dt} group by {unique_fields} having count(*)>1)"
  val hivecountDuplicate="select sum(cnt) from (select {unique_fields},(count(*)-1) cnt  from {schema_name}.{table_name} where " +
    "tbl_dt={tbl_dt} group by {unique_fields} having count(*)>1) a "
  val OriginalPartitionOverWrite="insert into {schema_name}.{table_name}  select * from {schema_name}.{table_name_tmp} where tbl_dt={tbl_dt}"
  //val Insert
  def main(args: Array[String]): Unit = {
    try {
      Seq("kinit","-k","-t","/etc/security/keytabs/daasuser.keytab","daasuser@MTN.COM")!;
      //      parsArgs(Array("--configFile", "C:/Testing/dedup/dedup.conf","--date","20181001"))
//      parsArgs(Array("--configFile", "/home/skhateeb/dedup/dedup.conf","--date","20180927","--feed","MSC_CDR"))
parsArgs(args)
      val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)
      if (date==0) Log.logError("No date is supplied", true)
      //parsArgs(Array("--configFile", "C:\\Testing\\FeedAdder\\Adder.conf"))
      Log.logMsg("Successfully Parsed args")
      val feedsList = parseMetadata(configFile)
      val regex = new Regex("\"(.*?)\"|([^\\s]+)")
      for (feed<-feedsList) {
        if (dedupFeed == feed._1) {
          var (exitValue, stdoutStream, stderrStream) = runCommand(Seq("hive", "-e", """SHOW COLUMNS IN """ + schema_name + "." + feed._1 +""";""",""""| awk -F" " '{print $1",\"}'"""))
          //val originalPartition=jDBCManager.executeSelectQuery(TempPartitionInsert,feed._1)
          val fieldList = regex.findAllMatchIn(stdoutStream).toList.mkString(",")
          println(feed._1 + "------>" + feed._2)
          val originalPartitionCount = OriginPartitionCount.replace("""{schema_name}""", schema_name).
            replace("""{table_name}""", feed._1).replace("""{tbl_dt}""", date.toString)
          Log.logMsg("Getting original count")

          val origCountRS = jDBCManager.executeSelectQuery(originalPartitionCount).toLong

          println(origCountRS + "--->" + originalPartitionCount)
          if (origCountRS.toLong > 0) {
            val countDuplication = CountDuplicate.replace("""{schema_name}""", schema_name).replace("""{table_name}""", feed._1).replace("{table_name_tmp}", feed._1 + "_tmp_" + date).replace("{unique_fields}", feed._2.UniqueFields).replace("""{tbl_dt}""", date.toString)
            Log.logMsg("Getting Duplicates count using Presto")
            var countDuplicationRS = 0L
            try {
              countDuplicationRS = jDBCManager.executeSelectQueryOnce(countDuplication).toLong
              println(countDuplicationRS + "--->" + countDuplication)
            } catch {
              case e: Exception => {
		 println(hivecountDuplicate.replace("""{schema_name}""", schema_name).replace("""{table_name}""", feed._1).replace("{table_name_tmp}", feed._1 + "_tmp_" + date).replace("{unique_fields}", feed._2.UniqueFields).replace("""{tbl_dt}""", date.toString))
                Log.logMsg("Presto query failed.Running Duplication count in hive")
                var (exitValue3, stdoutStream3, stderrStream3) = runCommand(Seq("hive", "-e", "set hive.tez.container.size=50000;set tez.runtime.io.sort.mb = 22096;set tez.runtime.io.sort.mb = 22096;" +countDuplication  +""" a;"""))
                println("hive dups" + stdoutStream3)
                println("hive dups" + stderrStream3)
                countDuplicationRS = stdoutStream3.trim.toLong
              }
            }
            if (countDuplicationRS.toLong > 0) {
              Log.logMsg("Creating temp table")
              val createTempTable = CreateTempTable.replace("""{schema_name}""", schema_name).
                replace("{table_name_tmp}", feed._1 + "_tmp_" + date).replace("""{table_name}""", feed._1).
                replace("""{tbl_dt}""", date.toString).replace("{field_List}", fieldList).replace("{unique_fields}", feed._2.UniqueFields)
              println(createTempTable)
              try {
		//val rsCreate = "Success"
                val rsCreate = jDBCManager.executeQueryOnce(createTempTable, schema_name + "." + feed._1 + "_tmp_" + date)
                println("=====>" + rsCreate)
                if (rsCreate != "Success") {
                  Log.logMsg("Creating temp table failed using presto, trying hive")
                  val createTempTable = "set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table " + schema_name + "." + feed._1 + " partition (tbl_dt) select " + regex.findAllMatchIn(stdoutStream).toList.mkString(",") + " from (select *,row_number() over( partition by " + feed._2.UniqueFields + ") as rank from " + schema_name + "." + feed._1 + " where tbl_dt=" + date + ") c where rank=1;"
                  var (exitValue2, stdoutStream2, stderrStream2) = runCommand(Seq("hive", "-e", "set hive.tez.container.size=50000;set tez.runtime.io.sort.mb=22096;set tez.runtime.io.sort.mb=22096;" + createTempTable))
                  println("hive temp: " + stdoutStream2)
                  println("hive temp: " + stderrStream2)
                }
              } catch {
                case e: Exception => {
                  Log.logMsg("temp table failed ")
                  val createTempTable = "set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table " + schema_name + "." + feed._1 + " partition (tbl_dt) select " + regex.findAllMatchIn(stdoutStream).toList.mkString(",") + " from (select *,row_number() over( partition by " + feed._2.UniqueFields + ") as rank from " + schema_name + "." + feed._1 + " where tbl_dt=" + date + ") c where rank=1;"
                  var (exitValue2, stdoutStream2, stderrStream2) = runCommand(Seq("hive", "-e", "set hive.tez.container.size=50000;set tez.runtime.io.sort.mb=22096;set tez.runtime.io.sort.mb=22096;" + createTempTable))
                }
              }
              val tempPartitionCount = TempPartitionCount.replace("""{schema_name}""", schema_name).
                replace("""{table_name}""", feed._1).replace("{table_name_tmp}", feed._1 + "_tmp_" + date).replace("""{tbl_dt}""", date.toString)
              Log.logMsg("Getting temp table count")
              val tmpCountRS = jDBCManager.executeSelectQuery(tempPartitionCount)
              println("------->" + tmpCountRS)
              val total = tmpCountRS.toLong + countDuplicationRS.toLong
              if (tmpCountRS.toLong > 0) {
                Log.logMsg("Temp table created")
                println(total + "=====>" + origCountRS)
                if (total == origCountRS) {
                  Log.logMsg("total is correct")
                  val dropOriginalPartition = DropOriginalPartition.replace("""{schema_name}""", schema_name).
                    replace("""{table_name}""", feed._1).replace("""{partition_field}""", "tbl_dt=" + date)
                  println("---->" + dropOriginalPartition)
                  //Seq("hive","-e",dropOriginalPartition)!;
                  try {
                    Log.logMsg("Running final query")
                    val originalPartitionOverWrite = OriginalPartitionOverWrite.replace("""{schema_name}""", schema_name).
                      replace("""{table_name}""", feed._1).replace("{table_name_tmp}", feed._1 + "_tmp_" + date).replace("""{tbl_dt}""", date.toString)
                    println("--->" + originalPartitionOverWrite)
                  //  val rsCreate = jDBCManager.executeFinalQuery(originalPartitionOverWrite)
                    //if (rsCreate != "Success") {
                      Log.logMsg("overwrite original table failed using presto, trying hive")
                      val lastQuery = "set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table " + schema_name + "." + feed._1 + " partition (tbl_dt) select * from " + schema_name + "." +feed._1 + "_tmp_" + date + " where tbl_dt=" + date
                      println("--->" + lastQuery)
                      var (exitValuef, stdoutStream2f, stderrStream2f) = runCommand(Seq("hive", "-e", lastQuery))
                        println("errors:"+stderrStream2f)
                    //}
                  } catch {
                    //enter hive
                    case e: Exception =>
                      Log.logMsg("overwrite original table failed using presto, trying hive")
                      val lastQuery = "set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table " + schema_name + "." + feed._1 + " partition (tbl_dt) select * from " + schema_name + "." + feed._1 + "_tmp_" + date + " where tbl_dt=" + date
                      println("--->" + lastQuery)
			
                      var (exitValuef, stdoutStream2f, stderrStream2f) = runCommand(Seq("hive", "-e", lastQuery))
			println("errors:"+stderrStream2f)
                  }
                }
                else {
                  Log.logMsg("Total (duplicates +  Temp table)<> original partition")
                }
              }
              else {
                Log.logMsg("Temp Table" + feed._1 + "_tmp_" + date + " does not have data")
              }
            }
            else {
              Log.logMsg(feed._1 + " does not have duplications for:" + date)
            }
            Log.logMsg("Successfully prepared Feeds Metadata list ")
          }
          Log.logMsg("Partition not found ")
        }
      }
    }
    catch {
      case e:Exception=> e.printStackTrace()
    }

    Log.logMsg("Finished")
    System.exit(0)
  }

  def runCommand(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  }

  def parsArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--configFile", argDataDate: String) => configFile = argDataDate
        case Array("--feed", argDataDate: String) => dedupFeed = argDataDate
        case Array("--date", argDataDate: String) => date = argDataDate.toInt
      }
    else
      Log.logError("No config file is supplied", true)
    if(dedupFeed.trim=="")  Log.logError("Please provide a feed to clean", true)
  }

  def parseMetadata(path: String):Map[String,Feed] = {
    var FeedMetadata = new ListBuffer[Feed]()
    var FeedMetadataMap=Map[String,Feed]()
    val config = ConfigFactory.parseFile(new File(path)).getConfig("Dedups.LoadCluster")
    val defaultConfig = ConfigFactory.load(config)
    val clusterName = config.getString("ClusterName")
    schema_name = config.getString("Schema")

    val feedsList=config.getObject("Feeds").entrySet().toList.map(entry => (entry.getKey, entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
    for(feed<-feedsList)
    {
      //      println(feed)
      val metadata = feed._2.toString.drop(1).dropRight(1)
      //      println(metadata)
      FeedMetadata +=  processfeeds(metadata)
      FeedMetadataMap+=((feed._1,processfeeds(metadata)))
    }
    println(FeedMetadataMap)
    FeedMetadataMap
  }



  def processfeeds(metadata: String): Feed = {
    val result = metadata.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(a => a.trim)
    var seq = 0
    var feed_name = ""
    var reg = ""
    var unique_fields = ""
    var ops = ""
    var hdfsCheck=""
    var rg=""
    var feeds_list=ListBuffer[String]()
    var minitar_filename=""
    for (a <- result) {
      val kv = a.split("=")
      if (kv(0).toLowerCase == "seq")
        seq = kv(1).toInt
      else if (kv(0).toLowerCase == "unique_fields")
        unique_fields = kv(1).replace("""|""",",")
    }
    return Feed( seq, unique_fields)
  }
}


class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

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

  def executeQueryOnce(query: String,table_name:String) :String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    var result=1
    var status="Start"
    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("dropping table: " +table_name)
        Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
        Log.logMsg("generating table statement for: "+table_name)
        statement = conn.createStatement()
        Log.logMsg("executing statementfor :"+ table_name)
        result = statement.executeUpdate(query)
        Log.logMsg("successfully populated"+table_name)
        return "Success"
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("reprocessing "+table_name)
        Log.logMsg("Exception while executing query: %s ".format(query))
        Log.logMsg("----------->"+ table_name+": "+ex.getMessage)
      }
    }
    status="Failed"
    status
  }
  def executeFinalQuery(query: String) :String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    var result=1
    var status="Start"
    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("generating table statement for final query")
        statement = conn.createStatement()
        result = statement.executeUpdate(query)
        return "Success"
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("Exception while executing query: %s ".format(query))
      }
    }
    status="Failed"
    status
  }

  def executeQuery(query: String,table_name:String) :String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    var result=1
    var status="Start"
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("dropping table: " +table_name)
          Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
          Log.logMsg("generating table statement for: "+table_name)
          statement = conn.createStatement()
          Log.logMsg("executing statementfor :"+ table_name)
          result = statement.executeUpdate(query)
          Log.logMsg("successfully populated"+table_name)
          return "success"
        }
      } catch {
        case ex: Throwable => {
          Log.logMsg("reprocessing "+table_name)
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          Log.logMsg("----------->"+ table_name+": "+ex.getMessage)
          //          Log.logMsg(ex.getStackTrace.toList.foreach(println))
          ex.printStackTrace
        }
      }
    }
    //(resultSet, statement)
    status="failed"
    Log.logMsg("************************** Failed processing for "+table_name+"*****************************************")
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


  def executeQuery(query: String) :String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    var result=1
    var status="Start"
    while ( retry < maxRetry) {
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
    status="failed"
    status
  }
}
object Log {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def logMsg(msg: String): Unit = {
    val time = LocalDateTime.now().format(formatter)
    println(time + " - " + msg)
  }

  def logError(msg: String, exit: Boolean, throwable: Throwable = null): Unit = {
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
    def toMapWithFields: Map[String,Any] = {
      val fields = (Map[String, Any]() /: c.getClass.getDeclaredFields) { (a, f) =>
        f.setAccessible(true)
        a + (f.getName -> f.get(c))
      }

      //s"${c.getClass.getName}(${fields.mkString(", ")})"
      fields
    }
  }
}
Deduplicate.main(args)

