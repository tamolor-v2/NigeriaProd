#!/bin/sh
exec scala -cp /mnt/beegfs/tools/CUPPA/lib/presto-jdbc-0.191.jar  -savecompiled "$0" "$@"
exec scala -cp  /mnt/beegfs/Deployment/DEV/scripts/BslScript/hive-jdbc-1.2.1000.2.6.1.0-129.jar -savecompiled "$0" "$@"
exec scala -cp ./hadoop-client-2.7.3.2.6.1.0-129.jar -savecompiled "$0" "$@"
!#


import java.io.File

import com.typesafe.config.ConfigFactory

import scala.sys.process
//import sun.rmi.runtime.Log
//import java.sql._
import java.io._
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.sys.process._

case class Cuppa_Components(seq:Int,componentName:String,componentType:String,connctionString:String,query:String,seqCommands:String,expectedResult:String)
object CUPPA {
  val HomeDir = "C:\\Testing\\CUPPA"
  var configFile: String = null
  def main(args: Array[String]): Unit = {
    Log.logMsg("Starting")
    parsArgs(Array("--configFile", "/home/daasuser/samer_scripts/CUPPA/cuppa.conf"))
    Log.logMsg("Successfully Parsed args")
    val componentsList = parseMetadata(configFile)
    Log.logMsg("Successfully prepared Components metadata list ")
	Seq("kinit", "-k", "-t", "/etc/security/keytabs/daasuser.keytab", "daasuser@MTN.COM") !;
    runComponents(componentsList)
  }

  def runComponents( componentsList:ListBuffer[Cuppa_Components])={
    for(a<-componentsList) {
      Log.logMsg("Checking "+a.componentName)
      if (a.componentType == "Seq") 
runSeq(a)
	else
runJDBC(a)
      
    }
  }

def runJDBC(component:Cuppa_Components)={
var currSeq=component.seqCommands.split("\\|")
val connString=component.connctionString.split("\\|")
val command=if(component.query!="None") currSeq:+component.query else currSeq
//println("==========================>"+connString.toList)
val jDBCManager= if(component.componentName=="Presto") 
		new JDBCManager(connString(0),connString(1),null)
	else
		new JDBCManager(connString(0),connString(1),connString(2))
    val result=jDBCManager.executeQuery(component.query)
//    println(result)
    if (result==component.expectedResult)
        logRunning(component.componentName)
    else
        logNotRunning(component.componentName)

}

def runSeq(component:Cuppa_Components)={
        var currSeq=component.seqCommands.split("\\|")
        val command=if(component.query!="None") currSeq:+component.query else currSeq
  //      println(command.toList)
        val ffcomm=command.toSeq
        var (exitValue, stdoutStream, stderrStream)=runCommand(ffcomm)
        //println(exitValue+" ----->    "+stdoutStream+":"+component.expectedResult+" =======>   "+stderrStream)
        if(component.expectedResult!="None"){
          if(exitValue==0 && stdoutStream.trim==component.expectedResult)
            logRunning(component.componentName)
          else
	logNotRunning(component.componentName)
	}else {
        if(exitValue==0 && stdoutStream.length>0)
          logRunning(component.componentName)
          else
          logNotRunning(component.componentName)
 }
}
def logRunning(componentName:String)={
 Log.logMsg("======>"+componentName+" is running")
}
def logNotRunning(componentName:String)={
 Log.logMsg("======>"+componentName+" is Not running")
}

def parseMetadata(path: String):ListBuffer[Cuppa_Components] = {
  var CuppaComponents = new ListBuffer[Cuppa_Components]()
  val config = ConfigFactory.parseFile(new File(path)).getConfig("CUPPA")
  val defaultConfig = ConfigFactory.load(config)
  //    val clusterName = config.getString("ClusterName")
  val components = config.getObject("Components").entrySet().toList.map(entry => (entry.getKey, entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
  for (component <- components) {

    val currComponent = component._1
    val metadata = component._2.toString.drop(1).dropRight(1)
    CuppaComponents+=prepareComponents(metadata,currComponent)
    //      println(currComponent+"--->"+metadata)
    //.split(",")
  }
  CuppaComponents
}

def prepareComponents(metadata:String,currComponent:String):Cuppa_Components={
  val result = metadata.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(a => a.trim)
  var componentType= ""
  var connectionString = ""
  var query = ""
  var seqCommands = ""
var expectedRes=""
  var seq=0
  for (a <- result) {
    //      println(a)
    val kv = a.split("=")
    if (kv(0).toLowerCase == "type")
      componentType = kv(1)
    else if (kv(0).toLowerCase.contains("connection_string")) {
      connectionString = kv(1)
    }
    else if (kv(0).toLowerCase == "query") {
	val queryLoc=metadata.indexOf("query=")+6
	val remainingString=metadata.substring(queryLoc,metadata.length)
      query = if(!metadata.contains("query=None")) metadata.substring(queryLoc,queryLoc+remainingString.indexOf(",")) else "None"
    }
    else if (kv(0).toLowerCase == "seq_commands") {
      seqCommands = kv(1)
    }
    else if (kv(0).toLowerCase == "number")
      seq = kv(1).toInt
    else if (kv(0).toLowerCase == "expected_result")
      expectedRes = kv(1)
  }
  return Cuppa_Components(seq,currComponent,componentType,connectionString,query,seqCommands,expectedRes)
}

def runCommand(cmd: Seq[String]): (Int, String, String) = {
  //println(cmd)
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
	println(url+","+user+","+pass)
 Seq("kinit", "-k", "-t", "/etc/security/keytabs/daasuser.keytab", "daasuser@MTN.COM") !;
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


