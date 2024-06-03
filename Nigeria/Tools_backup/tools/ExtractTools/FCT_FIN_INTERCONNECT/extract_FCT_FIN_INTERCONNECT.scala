#!/bin/sh
exec scala -cp /home/daasuser/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#
import sys.process._
import java.io._
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.Duration
import scala.io.Source
import collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source.fromFile
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.concurrent.{Executors, _}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
object Fct_in_inter {
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val Query=
    s"""
      |create table flare_8.fct_fin_interconnect_tmp AS select
      |DATE_KEY,
      |COUNT(*) CALL_COUNT,
      |SUBSTR(ANUM,1,6) as INCOMING_PREFIX,
      |SUBSTR(BNUM,1,6) as OUTGOING_PREFIX,
      |EVENT_DIRECTION,
      |INCOMING_PATH,
      |OUTGOING_PATH,
      |INCOMING_OPERATOR,
      |OUTGOING_OPERATOR,
      |INCOMING_PRODUCT,
      |OUTGOING_PRODUCT,
      |CASH_FLOW,
      |STATEMENT_DIRECTION,
      |SUM(EVENT_DURATION) EVENT_DURATION,
      |date_format(current_timestamp,'%Y%m%d %T') as create_dt,
      |DATE_KEY as tbl_dt
      |FROM flare_8.WBS_pm_rated_cdrs
      |WHERE 
      |tbl_dt between {yesterday} and {today}
      |GROUP BY
      |DATE_KEY,
      |SUBSTR(ANUM,1,6),
      |SUBSTR(BNUM,1,6),
      |EVENT_DIRECTION,
      |INCOMING_PATH,
      |OUTGOING_PATH,
      |INCOMING_OPERATOR,
      |OUTGOING_OPERATOR,
      |INCOMING_PRODUCT,
      |OUTGOING_PRODUCT,
      |CASH_FLOW,
      |STATEMENT_DIRECTION
    """.stripMargin
  var date=0
  def main(args: Array[String]): Unit = {
    val pool = Executors.newFixedThreadPool(4)
    Log.logMsg("start")
    parseArgs(args)
	Log.logMsg("Processing date"+date)
    import java.time.LocalDate
    val today= LocalDate.now.format(formatter).toInt
    val yesterday= LocalDate.now.minusDays(1).format(formatter).toInt
    //    val jDBCManager = new JDBCManager("jdbc:presto://10.1.197.145:8999/hive/flare_8", "test", null)
    Log.logMsg("dropping table flare_8.fct_fin_interconnect_tmp")
    Seq("hive", "-e",  "\"drop table if exists flare_8.fct_fin_interconnect_tmp\"") !;
    Log.logMsg("connecting to presto")
    val jDBCManager = new JDBCManager("jdbc:presto://10.1.197.145:8999/hive5/flare_8", "test", null)
if(date!=0) 
jDBCManager.executeQuery(Query.replace("""tbl_dt between {yesterday} and {today}""","tbl_dt="+date.toString))
else{
//    Log.logMsg("Executing  quety: "+Query.replace("""{yesterday}""",yesterday.toString).replace("""{today}""",today.toString))
    jDBCManager.executeQuery(Query.replace("""{yesterday}""",yesterday.toString).replace("""{today}""",today.toString))
}
    Log.logMsg("Running last query: ")
    Seq ("hive","-e","\"set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table flare_8.fct_fin_interconnect partition(tbl_dt) select * from flare_8.fct_fin_interconnect_tmp;") !;
    Log.logMsg("done")

  }
  def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--date", argDataDate: String) => date = argDataDate.toInt
      }
    else
      Log.logMsg("No date is supplied. Running for date range yesterday and today")
  }
}


// /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar



class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

  private val maxRetry = 5

  def executeQuery(query: String) :String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    var result=1
    var status="Start"
    val table_name="flare_8.fct_fin_interconnect_tmp"
    Log.logMsg("->->-> " +table_name)
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("dropping table: " +table_name)
          Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
          Log.logMsg("generating table statement for: "+table_name)
          statement = conn.createStatement()
          Log.logMsg("executing statement for :"+ table_name)
//	  Log.logMsg("executing statement: "+ query)
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
          //Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
        }
      }
    }
    //(resultSet, statement)
    status="failed"
    Log.logMsg("************************** Failed processing for "+table_name+"*****************************************")
    status
  }

  def close() {
    if (conn != null && !conn.isClosed)
      conn.close()
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

class ChunkBreaker {

  def break(fileContents: String): List[String] = {
    fileContents.split("""(?m)\s*(\r?\n\r?\n+)""").map(a=>a.trim).filterNot(_.isEmpty).toList
    //.map { case (a, b, c) => (a, c)}.filterNot(_._2.trim.length == 0)
  }
}
Fct_in_inter.main(args)

