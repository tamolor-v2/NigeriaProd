#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#
import sys.process._
import java.io._
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.Duration

import scala.io.Source
import collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source.fromFile
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.concurrent.{Executors, _}

object DPI_BSL {
  //val filesList=Array("C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectunpack.txt",
  //                    "C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectaggr.txt",
  //                    "C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectopco.txt",
  //                    "C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectpivot.txt")
  val filesList=Array(
    "/mnt/beegfs/tools/BSL_DPI/DPI_BSL_Queries_2/dpi_bsl_subjectunpack.txt",
    "/mnt/beegfs/tools/BSL_DPI/DPI_BSL_Queries_2/dpi_bsl_subjectaggr.txt",
    "/mnt/beegfs/tools/BSL_DPI/DPI_BSL_Queries_2/dpi_bsl_subjectopco.txt",
    "/mnt/beegfs/tools/BSL_DPI/DPI_BSL_Queries_2/dpi_bsl_subjectpivot.txt"
  )
  var date=new Array[Int](2)
  def main(args: Array[String]): Unit = {
    val pool = Executors.newFixedThreadPool(4)
    Log.logMsg("start")
    parseArgs(args)
for(dt<-date)  Log.logMsg("Processing DPI BSL for date(s): "+dt)
Seq ("hive","-e","\"use flare_8;msck repair table dpi_cdr;msck repair table dpi_cdr_unpack\"") !;
    //    val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive/flare_8", "test", null)
    for(file<-filesList) {
      val fileContents = Source.fromFile(file).getLines().mkString("\n")
    for(dt<-date){
      pool.execute(
        new Runnable {
          def run: Unit = {
            val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)
            val chunkBreaker = new ChunkBreaker
            val chunks = chunkBreaker.break(fileContents)
            //            Log.logMsg(chunks.length)
		println("=====>a")
            for (a <- chunks) {

              if (!a.toLowerCase.startsWith("drop")) {
                if (!a.toLowerCase.startsWith("insert")) {
                  Log.logMsg("=================================================================================")
                  Log.logMsg("executing query: \r\n" + a.replace("""{tbl_dt}""", dt.toString).replace("""{run_date}""",dt.toString).replace("create table flare_8","create table dpi_bsl").replace("CREATE TABLE flare_8","create table dpi_bsl"))
                  Log.logMsg("=================================================================================")
                  jDBCManager.executeQuery(a.replace("""{tbl_dt}""",dt.toString).replace("""{run_date}""",dt.toString).replace("create table flare_8","create table dpi_bsl").replace("CREATE TABLE flare_8","create table dpi_bsl"))
                }
                else if (a.toLowerCase.startsWith("insert")) {
                  Log.logMsg("Running last query: " + a)
                   Seq ("hive","-e","\"set hive.exec.dynamic.partition.mode=nonstrict;"+ a.replace("""{run_date}""",dt.toString)+"\"") !;
                }
              }
            }

          }
        })
    }
    }
    pool.shutdown()
    try {
      pool.awaitTermination(1,  TimeUnit.DAYS)
      //Log.logMsg("Processing of folder "+inputPath+" is finished")
    }
    catch {
      case e: Exception =>
    }
    Log.logMsg("done")

  }
  def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--date", argDataDate: String) => date = argDataDate.split(",").map(a=>a.trim.toInt)
      }
    else
      Log.logError("No date is supplied", true)
  }
}


class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

  private val maxRetry = 5

  def executeQuery(query: String) :String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
        Log.logMsg(query)
    var result=1
    var status="Start"
    val table_name=query.substring(query.toLowerCase.indexOf("create table")+13,query.toLowerCase.indexOf(" as")).replace("flare_8","dpi_bsl")
    Log.logMsg("->->-> " +table_name)
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("dropping table: " +table_name)
          Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
          Log.logMsg("generating table statement for: "+table_name)
          statement = conn.createStatement()
          Log.logMsg("executing statementfor: "+ table_name)
          result = statement.executeUpdate(query)
          Log.logMsg("successfully populated: "+table_name)
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
DPI_BSL.main(args)


