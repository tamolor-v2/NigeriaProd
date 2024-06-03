#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar  -savecompiled "$0" "$@"
exec scala -cp  /mnt/beegfs/Deployment/DEV/scripts/BslScript/hive-jdbc-1.2.1000.2.6.1.0-129.jar -savecompiled "$0" "$@"
exec scala -cp ./hadoop-client-2.7.3.2.6.1.0-129.jar -savecompiled "$0" "$@"
!#
import sys.process._
import java.io._
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.Duration
//import java.sql._
import scala.io.Source
import collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source.fromFile
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.concurrent.{Executors, _}
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
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path
//import java.io.PrintWriter

object CUPPA {
  val HomeDir = "C:\\Testing\\CUPPA"
  var configFile: String = null
    def main(args: Array[String]): Unit = {
      Log.logMsg("Starting")

var result=""
val hiveQuery="select 1 from flare_8.ggsn_cdr where tbl_dt=20180722 limit 1"
      val prestoQuery="select 1 from flare_8.ggsn_cdr where tbl_dt=20180722 limit 1"
      Log.logMsg("Parsing args")
      Log.logMsg("Checking Hive")
      val r =Seq ("hive","-e ","-S ", hiveQuery) !;
      println(r)
      if (r==0) Log.logMsg("Hive is running")
      else
	Log.logMsg ("Hive is Not running")
      Log.logMsg("Checking Presto")
      val jDBCManager= new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive4/flare_8", "test", null)
      result="1"
      result=jDBCManager.executeQuery(prestoQuery)
      println(result)
      if (result=="1")
	 Log.logMsg("Presto is running")
      else
         Log.logMsg("Presto is Not running")
      Log.logMsg("Checking HDFS")
        var (exitValueHdfs, stdoutStreamHdfs, stderrStreamHdfs)=runCommand(Seq ("hadoop", "fs","-ls","/"))
        //println(exitValue+"===>"+stdoutStream+"======>"+stderrStream)
        if(stderrStreamHdfs.length==0 && stdoutStreamHdfs.length>0 && exitValueHdfs==0)
          Log.logMsg("HDFS is running")
        else
          Log.logMsg("HDFS is NOT running")
         var (exitValueDfs, stdoutStreamDfs, stderrStreamDfs)=runCommand(Seq ("ls", "/mnt/beegfs/"))
        if(stderrStreamDfs.length==0 && stdoutStreamDfs.length>0 && exitValueDfs==0)
          Log.logMsg("DFS is running")
        else
          Log.logMsg("DFS is NOT running")

         //var (exitValueLoadCluster, stdoutStreamLoadCluster, stderrStreamLoadCluster)=runCommand(Seq ("bash","/home/daasuser/FlareCluster/Flare/bin/StatusKamanjaCluster.sh --ClusterId flarecluster --MetadataAPIConfig /home/daasuser/FlareCluster/Flare/config/MetadataAPIConfig.properties"))
var (exitValueLoadCluster, stdoutStreamLoadCluster, stderrStreamLoadCluster)=runCommand(Seq ("bash", "/home/daasuser/Scripts/StatusLoadCluster.sh"))
//println(stderrStreamLoadCluster.length + "=====>"+ stdoutStreamLoadCluster +"===>"+ exitValueLoadCluster)
	val upNodes=stdoutStreamLoadCluster.split("\r?\n").filter(a=>a.contains("UP")).length
	val downNodes=stdoutStreamLoadCluster.split("\r?\n").filter(a=>a.contains("DOWN")).length
        if( stdoutStreamLoadCluster.length>0 && exitValueLoadCluster==0 && upNodes>0){
          Log.logMsg("Load Cluster is running")
	}
        else
          Log.logMsg("Load Cluster is NOT running")
        Log.logMsg("Total Up Nodes="+upNodes)
        Log.logMsg("Total Down Nodes="+downNodes)

var (exitValueBslCluster, stdoutStreamBslCluster, stderrStreamBslCluster)=runCommand(Seq ("bash", "/home/daasuser/Scripts/StatusBslCluster.sh"))
        val upNodesBsl=stdoutStreamBslCluster.split("\r?\n").filter(a=>a.contains("UP")).length
        val downNodesBsl=stdoutStreamBslCluster.split("\r?\n").filter(a=>a.contains("DOWN")).length
        if( stdoutStreamBslCluster.length>0 && exitValueBslCluster==0 && upNodesBsl>0){
          Log.logMsg("Bsl Cluster is running")
        }
        else{
          Log.logMsg("Bsl Cluster is NOT running")
	}

        Log.logMsg("Total Up Nodes="+upNodesBsl)
        Log.logMsg("Total Down Nodes="+downNodesBsl)

         //var (exitValueHbase, stdoutStreamHbase, stderrStreamHbase)=runCommand(Seq ("echo list","\\|", "hbase shell"))
        //if(stderrStreamHbase.length==0 && stdoutStreamHbase.length>0 && exitValueHbase==0)
         // Log.logMsg("Hbase is running")
        //else
         // Log.logMsg("Hbase is NOT running")
//println(stderrStreamHbase.length + "=====>"+ stdoutStreamHbase +"===>"+ exitValueHbase)
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
      }
    else
      Log.logError("No config file is supplied", true)
  }
  def checkPidFile(pidFile: String): Boolean = {
    new File(pidFile).exists()
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
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          statement = conn.createStatement()
      //    result = statement.executeUpdate(query)
resultSet = statement.executeQuery(query)
while (resultSet.next()){
          return resultSet.getString(1) 
}
        }
      } catch {
        case ex: Throwable => {

          retry = retry + 1
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))

                    //Log.logMsg(ex.getStackTrace.toList.foreach(println))
          ex.printStackTrace
        }
      }
    }
    //(resultSet, statement)
    status="failed"
    status
  }

  def close() {
    if (conn != null && !conn.isClosed)
      conn.close()
  }
}
CUPPA.main(args)
