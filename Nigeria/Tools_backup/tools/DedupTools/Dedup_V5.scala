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
import scala.util.matching.Regex

object DeduplicationScript {
  var configFile: String =""
  var feedsMetadata:String=""
  case class Feed(seq:Int,UniqueFields:String)
  var schema_name = ""
  var date=0
  var dedupFeed=""
  var temp_table_name=""
  var HadoopFolder=""
  val CreateTempTable=   """create table {schema_name}.{table_name_tmp} as select {field_List} from( select *,row_number() over( partition by {unique_fields}) as rank from {schema_name}.{table_name} where tbl_dt= {tbl_dt}) c where rank=1"""
  val insertTempTable=   """insert into {schema_name}.{table_name_tmp}  select {field_List} from( select *,row_number() over( partition by {unique_fields}) as rank from {schema_name}.{table_name} where tbl_dt= {tbl_dt}) c where rank=1"""
  val OriginPartitionCount="select count(*) from {schema_name}.{table_name} where tbl_dt= {tbl_dt} "
  val TempPartitionCount="select count(*) from {schema_name}.{table_name_tmp} where tbl_dt= {tbl_dt} "
  val DropOriginalPartition="ALTER TABLE {schema_name}.{table_name} DROP IF EXISTS PARTITION ({partition_field})"
  val CountDuplicate="select sum(cnt) from (select {unique_fields},(count(*)-1) cnt  from {schema_name}.{table_name} where " +
    "tbl_dt={tbl_dt} group by {unique_fields} having count(*)>1)"
  val hivecountDuplicate="select sum(cnt) from (select {unique_fields},(count(*)-1) cnt  from {schema_name}.{table_name} where " +
    "tbl_dt={tbl_dt} group by {unique_fields} having count(*)>1) a "
  val OriginalPartitionOverWrite="insert into {schema_name}.{table_name}  select * from {schema_name}.{table_name_tmp} where tbl_dt={tbl_dt}"
  var fieldList=""
  val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)

  def main(args: Array[String]): Unit = {
    Log.logMsg("Started")
//    parsArgs(Array("--configFile", "C:/Testing/dedup/dedup.conf","--date","20180927","--feed","MSC_CDR"))
    parsArgs(args)
    if (date==0) Log.logError("No date is supplied", true)
    Seq("kinit","-k","-t","/etc/security/keytabs/daasuser.keytab","daasuser@MTN.COM")!;
    Log.logMsg("Successfully Parsed args")
    val feedsList = parseMetadata(configFile)
    val regex = new Regex("\"(.*?)\"|([^\\s]+)")
    val (feed,data)=getFeedInfo(feedsList)
    if (feed=="") Log.logError("Feed couldn't be found in config file",true) else Log.logMsg(feed+" Feed Metadata found")
        temp_table_name=feed+"_tmp_"+date.toString

val tmpTableStartExists=jDBCManager.tableExists(schema_name,temp_table_name)
if(tmpTableStartExists){
      val tmpCountForStart=getCount(feed,TempPartitionCount,data)
     Log.logMsg("Count for Temp table= "+tmpCountForStart)

        if(tmpCountForStart>0  ) {
                 Log.logMsg("Temp table already has data, the script will terminate. please contact Ops technical lead for further instructions")
        System.exit(1)
        }
}

     val columns =extractColumnsNames(schema_name+"."+feed)
    temp_table_name=feed+"_tmp_"+date.toString
     fieldList = columns.mkString(",")
    val duplicateCount=getCount(feed,CountDuplicate,data)
    if(duplicateCount>0){
      Log.logMsg("Count of duplicates = "+duplicateCount) 
      val originalCount=getCount(feed,OriginPartitionCount,data)
     Log.logMsg("Count of Original Partition = "+originalCount)
      Log.logMsg("Creating temp table")
      val createTemp=createTempTable(feed,CreateTempTable,insertTempTable,data)
      val tmpCount=getCount(feed,TempPartitionCount,data)
     Log.logMsg("Count for Temp table= "+tmpCount)
	if(originalCount==(tmpCount+duplicateCount)){
		Log.logMsg("The total for (tmpCount+duplicateCount) = OriginalPartitionCount: "+(tmpCount+duplicateCount) +"="+originalCount)
		var (exitCode,stdout,stderr)=deletePartition(feed)
		if (exitCode==0 ){
			
			Log.logMsg("Old data moved to trash at: "+stderr.split("\n")(0).substring(stderr.split("\n")(0).indexOf("to trash at:")+13,stderr.split("\n")(0).length))
			populatePartition(feed,OriginalPartitionOverWrite)
			 
		}
		else{
			 Log.logMsg("Old data wasn't deleted... Exitting")
			System.exit(0)
		}
	}
	else{
		Log.logMsg("The total for (tmpCount+duplicateCount) <> OriginalPartitionCount: "+(tmpCount+duplicateCount) +"<>"+originalCount)
	}
    }else
      Log.logMsg("No Duplication found for "+feed+" in partition:"+date)
    Log.logMsg("Finished")
}

def populatePartition(feed:String,sqlQuery:String)={
val query=sqlQuery.replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("{table_name_tmp}", temp_table_name)
      .replace("""{tbl_dt}""", date.toString)
 Log.logMsg("Executing Query: "+query)
 jDBCManager.executeQueryOnce(query, feed)
}

def deletePartition(feed:String):(Int,String,String)={
println("hadoop fs -rm "+HadoopFolder+"/"+feed+"/tbl_dt="+date+"/D*")
var (exitValue, stdoutStream, stderrStream) = runCommand(Seq("hadoop","fs","-rm",HadoopFolder+"/"+feed+"/tbl_dt="+date+"/D*"))
println("Exit="+exitValue)
println("Out="+stdoutStream.split("\n")(0))
println("Err="+stderrStream.split("\n")(0).substring(+stderrStream.split("\n")(0).indexOf("to trash at:")+13,stderrStream.split("\n")(0).length))
(exitValue,stdoutStream,stderrStream)
}
 def extractColumnsNames(feed_name:String)={
    var (exitValue, stdoutStream, stderrStream) = runCommand(Seq("/opt/presto/bin/presto","--server","master01004:8099", "--catalog","hive5","--execute", "show columns from "+feed_name))
    val columns=stdoutStream.split("\n").map(a=>a.split(",")(0).replace("\"","")).toList
    columns

//    val columns=show.split("\n").map(a=>a.split(",")(0).replace("\"","")).mkString(",")
//    columns
  }

 def getFeedInfo(feedList:Map[String,Feed]):(String,Feed)={
    val feed=feedList.getOrElse(dedupFeed, new Feed(0,""))
      return (dedupFeed,feed)
  }

def createTempTable(feed:String, sqlCreateQuery:String,sqlInsertQuery:String, data:Feed)={
  var query=""
  val tmpTableExists=jDBCManager.prepareTempTable(schema_name,temp_table_name)
  //val tmpTableExists=true
  if (tmpTableExists==false) {
    query=sqlCreateQuery.replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("{table_name_tmp}", temp_table_name)
      .replace("{unique_fields}", data.UniqueFields).
      replace("""{tbl_dt}""", date.toString).replace("""{field_List}""",fieldList)
  }
  else {
     query= sqlInsertQuery.replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("{table_name_tmp}", temp_table_name)
      .replace("{unique_fields}", data.UniqueFields).
      replace("""{tbl_dt}""", date.toString).replace("""{field_List}""",fieldList)
  }
  Log.logMsg("Executing Query: "+query)
  jDBCManager.executeQueryOnce(query, temp_table_name)
}
def getCount(feed:String,sqlQuery:String,data:Feed):Long={
var finalCount=0L
    val query=sqlQuery.replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("{table_name_tmp}", temp_table_name)
      .replace("{unique_fields}", data.UniqueFields).
      replace("""{tbl_dt}""", date.toString)
Log.logMsg("Executing query: "+query)
    val count=jDBCManager.executeSelectQuery(sqlQuery.replace("""{schema_name}""", schema_name).replace("""{table_name}""", feed).replace("{table_name_tmp}", feed + "_tmp_" + date).replace("{unique_fields}", data.UniqueFields).replace("""{tbl_dt}""", date.toString))
try{
val qCount=if(count!=None) count.toLong else 0L

finalCount=qCount.asInstanceOf[Long]
return finalCount
}
 catch {
    case ex: Throwable =>{
	Log.logMsg("Count=0L")
	return 0L
}
}
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
    HadoopFolder= config.getString("HadoopFolder")
    val feedsList=config.getObject("Feeds").entrySet().toList.map(entry => (entry.getKey, entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
    for(feed<-feedsList)
    {
      val metadata = feed._2.toString.drop(1).dropRight(1)
      FeedMetadata +=  processfeeds(metadata)
      FeedMetadataMap+=((feed._1,processfeeds(metadata)))
    }
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
def tableExists(schema_name:String, table_name:String):Boolean={
  val query="show tables from "+schema_name+""" like '%"""+ table_name.toLowerCase+"""%'"""
        Log.logMsg("Executing query: "+query)
  var statement: Statement = null
  var resultSet: ResultSet = null
//  var result=1
  var status="Start"
        Log.logMsg("Checking if temp table exists")

  try {
    if (conn != null && !conn.isClosed) {
      statement = conn.createStatement()
      val result = statement.executeQuery(query)

      val tableStatus=if(result.next()) result.getString(1) else ""
      if (tableStatus.trim.length>0) {
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
DeduplicationScript.main(args) 
