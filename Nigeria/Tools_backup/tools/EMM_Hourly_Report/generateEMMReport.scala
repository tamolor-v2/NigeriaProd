#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
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
import java.io.{File, _}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import sun.rmi.runtime.Log

import scala.sys.process.ProcessLogger
//import shapeless.ops.nat.GT.>
//import sun.rmi.runtime.Log
import scala.sys.process.{ProcessLogger, _}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.io.Source.fromFile
import scala.util.matching.Regex
import java.util.{Calendar, Date}
case class EMMInfo(var tbl_dt:String="",var hour:String="",var ReceivedFiles:String="",var DoneFiles:String="",var ProcessedRecords:String="")

object EMMReport{

  val myQuery="""select c.tbl_dt, c.hour, d.number_of_received_files, c.number_of_done_files , c.number_of_records_processed
                |from 
                |(select b.tbl_dt, substring(b.endtime,12,2) as hour, count(distinct b.filename) number_of_done_files, sum(processedrecordscount) number_of_records_processed 
                |from flare_8.file_stats b 
                |where tbl_dt = {tbl_dt} and cast(substring(b.endtime,12,2) as Integer) ={run_hour}  and status='Success'
                |group by tbl_dt,substring(b.endtime,12,2)) c 
                |inner join
                | (select a.tbl_dt, substring(a.starttime,12,2) as hour, count(distinct a.filename) number_of_received_files 
                | from flare_8.file_stats a 
                | where tbl_dt = {tbl_dt} and cast(substring(a.starttime,12,2) as Integer)={run_hour} and status='Success'
                | group by tbl_dt, substring(a.starttime,12,2)) d 
                | on c.tbl_dt = d.tbl_dt 
                | and c.hour = d.hour 
                | order by c.tbl_dt, c.hour""".stripMargin

def main(args: Array[String]): Unit = {
	var eMMInfo:EMMInfo=new EMMInfo("","","","","")
	val now = Calendar.getInstance()
	val currentRunTime=TimeHelper.getCurrentTime()
	val previousHour = now.get(Calendar.HOUR_OF_DAY)-1
	val formattter = DateTimeFormatter.ofPattern("yyyyMMdd")
	val tbl_dt= LocalDateTime.now().format(formattter)
	val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)
	val rplcQuery=myQuery.replace("""{run_hour}""",previousHour.toString).replace("""{tbl_dt}""",tbl_dt)
	val result=jDBCManager.executeQuery(rplcQuery)
//	println(rplcQuery)
//	println("=====>"+result.toString)
while (result.next())
	{
	//	println(result.getString(1)+"|"+result.getString(2)+"|"+result.getString(3)+"|"+result.getString(4)+"|"+result.getString(5))
	eMMInfo.tbl_dt=result.getString(1)
	eMMInfo.hour=result.getString(2)
	eMMInfo.ReceivedFiles=result.getString(3)
	eMMInfo.DoneFiles=result.getString(4)
	eMMInfo.ProcessedRecords=result.getString(5)
	}	
val finishRunTime=TimeHelper.getCurrentTime()
println(eMMInfo+"|"+currentRunTime+"|"+finishRunTime)
val content=HtmlHelper.prepareEmail(eMMInfo,currentRunTime,finishRunTime)
val filePath="/mnt/beegfs/tools/EMM_Hourly_Report/data/Hour_"+eMMInfo.tbl_dt+"_"+eMMInfo.hour+"_"+TimeHelper.getCurrentTime_F()
FileHelper.writeNewFiles(filePath,content)
EmailSender.sendEmail(filePath,"EMM Files Stats","EMM files Stats")
}
}



class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

  private val maxRetry = 3

  def executeQuery(query: String): ResultSet = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    while (resultSet == null && retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
		println("executing Query")
          statement = conn.createStatement()
          resultSet = statement.executeQuery(query)
        }
      } catch {
        case ex: Throwable => {
          retry = retry + 1
          Log.logMsg("Exception while executing query:  "+ ex)
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
        }
      }
    }
    resultSet
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
object TimeHelper {
  val fileFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HHmmss.SSS")
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val todayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def getCurrentTime(): String = {
    val time = LocalDateTime.now().format(formatter)
    time
  }

  def getCurrentTime_F(): String = {
    val time = LocalDateTime.now().format(fileFormatter)
    time
  }

  def getCurrentTime_today(): String = {
    val time = LocalDateTime.now().format(todayFormatter)
    time
  }
}
object HtmlHelper {

  def getFooter():String={
    val htmlFooter = new StringBuilder
    htmlFooter.append("</table>").append("\n").append("</body>").append("\n")

    htmlFooter.toString
  }
  def getHeader(ExceptionHeader:String):String={
    val htmlHeader = new StringBuilder
    htmlHeader.append("""<table border="1" cellspacing="5" cellpadding="1">""").stripMargin
    //    htmlHeader.append("""<table style="width:50%" border="1">""").append("\n")
    val headerColumns=ExceptionHeader.split(",")
    if(headerColumns.length>0)
      for(e<-headerColumns)
        htmlHeader.append(s"""<th align='center' colspan='2'  >${e}</th>""").append("\n")
    htmlHeader.toString
  }
  def writeInTableCell(k:String,v:String):String={
    return s"""<tr><td><p><strong>${k}</td><td> ${v}</td></tr>\n"""
  }
  def prepareEmail(emm:EMMInfo,runDate:String,finishTime:String):ListBuffer[String]={
    val htmlAlerts=new ListBuffer[String]()
      val alertString = new StringBuilder
      alertString.append(getHeader("Previous Hour Stats"))
      alertString.append(writeInTableCell("Run Time",runDate)).append("\n")
      alertString.append(writeInTableCell("End Time",finishTime)).append("\n")
      alertString.append(writeInTableCell("tbl_dt",emm.tbl_dt)).append("\n")
      alertString.append(writeInTableCell("hour",emm.hour)).append("\n")
      alertString.append(writeInTableCell("ReceivedFiles",emm.ReceivedFiles)).append("\n")
      alertString.append(writeInTableCell("DoneFiles",emm.DoneFiles)).append("\n")
      alertString.append(writeInTableCell("ProcessedRecords",emm.ProcessedRecords)).append("\n")
      alertString.append(getFooter())
   alertString.toString.split("\n").to[ListBuffer]
  }
}



object FileHelper {
  val FILE_SEPARATOR: String = System.getProperty("file.separator")

  private def getFile(path: String): File = {
    if (path == null || path.length == 0) {
      throw new IllegalArgumentException(s"Path is not valid ($path)")
    }
    new File(path)
  }

  def moveFile(p1: String, p2: String): Unit = {
    val f1 = getFile(p1)
    val f2 = getFile(p2)
    Files.move(f1.toPath, f2.toPath, StandardCopyOption.REPLACE_EXISTING)
  }

  def getStringFromFile(path: String): String = {
    val file = getFile(path)
    if (!file.exists())
      throw new FileNotFoundException(file.getAbsolutePath)

    fromFile(file).mkString

  }

  def recursiveListFiles(f: File): Array[File] = {
    val currentDir = f.listFiles
    currentDir ++ currentDir.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getLinesFromFile(path: String): List[String] = {
    fromFile(getFile(path)).getLines().toList
  }

  def writeNewFiles(fileName: String, reports: ListBuffer[String]) = {
    val alertFileList = new ListBuffer[String]
    try {
      var exceptionSeq = 0
        val currentdt=TimeHelper.getCurrentTime_F()
        exceptionSeq += 1
        Log.logMsg("new Alert File" + exceptionSeq + ":" + fileName )
        val bw = new BufferedWriter(new FileWriter(new File(fileName )))
//	println(reports)
//	System.exit(0)
	for (l<-reports)
        bw.write(l)
        bw.close()
    }
    catch {
      case e: Exception => {
        throw new Exception(s"Exception while writing into file ${fileName}", e)
      }
    }
  }

  def writeMap(history_LineCont: scala.collection.mutable.Map[String, Int], statsFile: String) = {
    try {
      val bw = new BufferedWriter(new FileWriter(new File(statsFile)))
      for (a <- history_LineCont) {
        bw.write(a._1 + "," + a._2 + "\r\n")
      }
      bw.close()
    }
    catch {
      case e: Exception => {
        throw new Exception(s"Exception while writing into file ${statsFile}", e)
        //Log.logError(s"Exception while writing into file ${fileName}", e, true)
      }
    }
  }

  def writeNewFile(fileName: String, content: List[String], append: Boolean): Unit = {
    val file = getFile(fileName)
    try {
      val bw = new BufferedWriter(new FileWriter(file, append))
      content.foreach(f => {
        bw.write(f)
        bw.newLine
      }
      )
      bw
        .close()
    }
    catch {
      case e: Exception => {
        throw new Exception(s"Exception while writing into file ${fileName}", e)
        //Log.logError(s"Exception while writing into file ${fileName}", e, true)
      }
    }
  }

  def getSubFileDirs(path: String, f: File => Boolean): List[File] = {
    val file = getFile(path)
    if (!file.exists || !file.isDirectory)
      throw new IllegalArgumentException(s"Path is not a valid directory ($path)")
    else
      file.listFiles.filter(f(_)).toList
  }

  def mkDir(path: String): Unit = {
    val file = getFile(path)
    file.mkdirs
  }

}

object EmailSender {
  import sys.process._
  private val WORK_DIR = "/mnt/beegfs/tools/EMM_Hourly_Report/Email/"
  private val HEADER_FILE = WORK_DIR + "mailheader"
  private val SEND_EMAIL_SCRIPT = WORK_DIR + "sendEmail.sh"
  def sendEmail(EMM_Report:String,subject:String,emailType:String): Unit = {
    val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
    val headerText = FileHelper.getStringFromFile(HEADER_FILE)
    val mailHeader = headerText.replace("${SUBJECT}", subject+"_"+time)
    val dataDt=TimeHelper.getCurrentTime_today()
    val historyEmailDir = WORK_DIR + "History" + FileHelper.FILE_SEPARATOR + dataDt
    FileHelper.mkDir(historyEmailDir)
    val emailHeader = historyEmailDir + FileHelper.FILE_SEPARATOR + "header_" + time
    FileHelper.writeNewFile(emailHeader, List(mailHeader), false)
    val summaryEmailBody = historyEmailDir + FileHelper.FILE_SEPARATOR + emailType + "_" + dataDt + "_summary_" + time + ".html"
Seq("ssh", "edge01002", "bash", SEND_EMAIL_SCRIPT, emailHeader,EMM_Report) !;
  }
}
EMMReport.main(args)
