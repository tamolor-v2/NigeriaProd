#!/bin/sh
exec scala -cp /home/daasuser/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
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
object PrestoETLExecutor {
  case class QuerySet(seq:Int,UniqueFields:String,var totalQueries:Int=0)
  var configFile: String =""
  var jDBCManager:JDBCManager = null
var PrestoConnString=""
  var PrestoUID=""
  var PrestoPWD:String=null
  //new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)
  var krb:Seq[String]=null
  def main(args: Array[String]): Unit = {
    parsArgs(args)
    val ETLConfig = parseMetadata(configFile)
    kerberosAuth(krb)
    jDBCManager =new JDBCManager(PrestoConnString, PrestoUID, PrestoPWD)
	val msc_daasCount=jDBCManager.getCount("select count(*) from flare_8.msc_daas where tbl_dt=20181231")
	Log.logMsg("Count="+msc_daasCount)
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

  def kerberosAuth(krb:Seq[String])={
    runCommand(krb)
  }
  def parsArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--configFile", argDataDate: String) => configFile = argDataDate
      }
    else
      Log.logError("No config file is supplied", true)
  }

  def parseMetadata(path: String):Map[String,QuerySet] = {
    var FeedMetadata = new ListBuffer[QuerySet]()
    var FeedMetadataMap=Map[String,QuerySet]()
    val config = ConfigFactory.parseFile(new File(path)).getConfig("ETLExecutor.LoadCluster")
    val defaultConfig = ConfigFactory.load(config)
    val clusterName = config.getString("ClusterName")
    krb=config.getString("Kerberos").split("\\|").toSeq
    PrestoConnString=config.getString("PrestoConnString")
    PrestoUID=config.getString("PrestoUID")
    PrestoPWD=if (config.getString("PrestoPWD").equalsIgnoreCase("null")) null else config.getString("PrestoPWD")
    val querySet=config.getObject("Steps").entrySet().toList.map(entry => (entry.getKey, entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
    for(query<-querySet)
    {
      val metadata = query._2.toString.drop(1).dropRight(1)
      FeedMetadataMap+=((query._1,processQueryInfo(metadata)))
    }
    FeedMetadataMap.foreach(println)
    FeedMetadataMap
  }

  def processQueryInfo(metadata: String): QuerySet = {
    val result = metadata.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(a => a.trim)
    var seq = 0
    var queriesPath = ""
    var feeds_list=ListBuffer[String]()
    var minitar_filename=""
    for (a <- result) {
      val kv = a.split("=")
      if (kv(0).toLowerCase == "seq")
        seq = kv(1).toInt
      else if (kv(0).toLowerCase == "queryfilepath")
        queriesPath = kv(1).replace("""|""",",")
    }
    return QuerySet( seq, queriesPath,queriesPath.split(",").length)
  }


}


class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private var conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

  private val maxRetry = 1
  def createConnection(url:String,user:String,pwd:String)={
    this.conn=DriverManager.getConnection(this.url, this.user, this.pass)
  }

  def getCount(query:String):Long={
    var statement: Statement = null
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    if (conn != null && !conn.isClosed) {
      Log.logMsg("Executing count statement: "+query)
      statement = conn.createStatement()
      val result = statement.executeQuery(query)
      val cnt = if (result.next) result.getString(1).toLong else 0L
      return cnt
    }
    return 0L
  }


  def tableExists(table_name:String,schema_name:String): Boolean ={
    val query="show tables from "+schema_name+""" like '%"""+ table_name.toLowerCase+"""%'"""
    Log.logMsg("Executing query: "+query)
    var statement: Statement = null
    var resultSet: ResultSet = null
    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("Checking if table exists")
        statement = conn.createStatement()
        val result = statement.executeQuery(query)

        val tableStatus=if(result.next()) result.getString(1) else ""
        if (tableStatus.trim.length>0 && !tableStatus.contains("failed") && tableStatus!="") {
          Log.logMsg("Table "+schema_name+"."+table_name+ " exists")
          return true
        }
        else if(tableStatus.trim==""){
          Log.logMsg("Couldn't find table: "+schema_name+"."+table_name)
          return false
        }else{
          Log.logMsg("Failed :"+tableStatus)
          return false
        }
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("Exception while executing: %s ".format(query))
        Log.logMsg( table_name+": "+ex.getMessage)
      }
    }
    false
  }

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
  def prepareTempTable(schema_name:String, table_name:String):Boolean={
    val query="show tables from "+schema_name+""" like '%"""+ table_name.toLowerCase+"""%'"""
    Log.logMsg("Executing query: "+query)
    var statement: Statement = null
    var resultSet: ResultSet = null
    //  var result=1
    var status="Start"
    Log.logMsg("Checking if temp table exists")

    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("Checking if temp table exists")
        statement = conn.createStatement()
        val result = statement.executeQuery(query)

        val tableStatus=if(result.next()) result.getString(1) else ""
        if (tableStatus.trim.length>0) {
          Log.logMsg("Table exists, deleting Data")
          Log.logMsg("delete from "+schema_name + "."+table_name)
          statement.executeUpdate("delete from "+schema_name + "."+table_name)
          return true
        }
        else
          return false
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("reprocessing "+table_name)
        Log.logMsg("Exception while executing query: %s ".format(query))
        Log.logMsg("----------->"+ table_name+": "+ex.getMessage)
      }
    }
    false
  }
  def executeQueryOnce(query: String,table_name:String) :String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    var result=1
    var status="Start"
    try {
      if (conn != null && !conn.isClosed) {
        Log.logMsg("generating table statement for: "+table_name)
        statement = conn.createStatement()
        Log.logMsg("executing statement :"+ table_name)
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
        Log.logMsg("generating table statement for final query\r\n"+query)
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

PrestoETLExecutor.main(args)
