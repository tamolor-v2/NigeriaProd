#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#
import java.io.{File, _}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import javax.xml.transform.Templates

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source.fromFile
//import sun.rmi.runtime.Log

case class KamLogException(clusterName:String,logPtah:String,ExceptionType:String, searchString:String, msg:String, severity:Int, priority:Int,repetition:Int=0,totalLogLines:Int=0)

object SwiftAlert {
  var configFile: String = null
  var headerColumns=""
  var outputDir=""

  def main(args: Array[String]): Unit = {
    //    parsArgs(Array(""))
    val time=TimeHelper.getCurrentTime()
    Log.logMsg("Starting")
    parsArgs(args)
    val exceptionList=parseMetadata(configFile)
    val finalList=processExceptionList(exceptionList)
    val final_tuple=finalList flatMap KamLogException.unapply
    val finalMap=final_tuple.groupBy(rc=>rc._1+"-"+rc._3).map { case (k,v) => (k, (v(0)._4,v(0)._5,v(0)._6,v(0)._7,v(0)._8,v(0)._9))}
//    finalMap.foreach(a=>println(a._2._1))
//    finalMap.foreach(println)
val fMap=cleanExceptions(finalMap)
    val alertList=HtmlHelper.getAlertsList(fMap,time,TimeHelper.getCurrentTime())
//    println(alertList)
//    val k=HtmlHelper.getHtmlTable(fMap,headerColumns)
//    println(k)
    val alertFiles=FileHelper.writeNewFile(outputDir,alertList)
    println(alertFiles)
    Log.logMsg("Finished")
  }

def processExceptionList(exceptions: ListBuffer[KamLogException]):ListBuffer[KamLogException]={
  var ExceptionFinalReport=new ListBuffer[KamLogException]()
  val fileLocation=exceptions.head.logPtah
  var fileList=recursiveListFiles(new File(fileLocation))
//  fileList.foreach(println)
    for (a<-exceptions){
      var count=0
      var totalLines=0
//      println(a.searchString+"---->"+a.msg)
//      println("====>"+a)
      if(a.logPtah!=fileLocation)
       fileList=recursiveListFiles(new File(fileLocation))
      count= grep(a.searchString,fileList)._1
      totalLines=grep(a.searchString,fileList)._2

      if(count<=0)
        ExceptionFinalReport+=a.copy(totalLogLines = a.totalLogLines+totalLines)
      else
        ExceptionFinalReport+=a.copy(totalLogLines = a.totalLogLines+totalLines,repetition =a.repetition+count)
  }
    ExceptionFinalReport
}

  def parsArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--configFile", argDataDate: String) => configFile = argDataDate
      }
    else
      Log.logError("No config file is supplied", true)
  }
def cleanExceptions(ExceptionFinalReport:Map[String,(String,String,Int,Int,Int,Int)]):Map[String,(String,String,Int,Int,Int,Int)]={
  val ExceptionFinal = ExceptionFinalReport.filter(_._2._5>0  )
//  for(e<-ExceptionFinal){
//    println(e)
  ExceptionFinal
  }

  def parseMetadata(path: String) = {
    var ExceptionFinalReport=new ListBuffer[KamLogException]()

    val config = ConfigFactory.parseFile(new File(path)).getConfig("Clusters.LoadCluster")
    val defaultConfig = ConfigFactory.load(config)
//    println(config.getString("ClusterName"))
    val clusterName=config.getString("ClusterName")
    val logPath=config.getString("logsPath")
     outputDir=config.getString("outputDir")
    headerColumns=config.getString("HeaderColumns")
//    val dfsExceptions=config.getConfig("Exceptions").entrySet().toList.map(entry => (entry.getKey,entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
    val dfsExs2=config.getObject("Exceptions").entrySet().toList.map(entry => (entry.getKey,entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
//    val logEx=config.getObject("Exceptions")
//    println(dfsExs2)
      for(logException<-dfsExs2) {
        val exceptionType=logException._1
        val metadata=logException._2.toString.drop(1).dropRight(1)//.split(",")
//        println(logException)
        ExceptionFinalReport+=processExceptions(clusterName,logPath,exceptionType,metadata)


      }
//    ExceptionFinalReport.foreach(println)
    // searchString

    ExceptionFinalReport
  }
  def processExceptions(clusterName:String, logPath:String, exceptionType:String, metadata:String ):KamLogException= {
//    println(metadata)
    val result = metadata.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(a=>a.trim)
//    println(result.toList)
    var msg=""
    var searchString=""
    var severity=0
    var priority=0
    val totalLogLines=0
    var repetion=0
//for(kv<-result)
    for (a <- result) {
//      println(a)

val kv=a.split("=")
//      println("======>"+kv(0))
if(kv(0).toLowerCase=="msg")
  msg=kv(1)
else if (kv(0).toLowerCase.contains( "searchstring")) {
//  println("======>"+searchString)
  searchString = kv(1)
}
else if (kv(0).toLowerCase=="severity")
  severity=kv(1).toInt
else if (kv(0).toLowerCase=="priority")
  priority=kv(1).toInt
    }
//    println(searchString+"---->"+msg)
//    println("---------------------------------------")
//    println(clusterName+","+logPath+","+exceptionType,searchString,msg,severity,priority,repetion,totalLogLines)
    return  KamLogException(clusterName,logPath,exceptionType,searchString,msg,severity,priority,repetion,totalLogLines)
  }

  def recursiveListFiles(f: File): Array[File] = {
    val currentDir = f.listFiles
    currentDir ++ currentDir.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def matchLines(source: List[String], pattern: String,file:File):Int = {
//    source.getLines.zipWithIndex.filter(l => pattern.r.findFirstIn(l._1) != None)

    var count=0
    for(l <-source) {
      if(l.contains(pattern))
        count+=1
//      val pt=pattern.r.findFirstIn(l)
//      if (pattern.r.findFirstIn(l) != None)
    }
//    println(file+"=======> pattern: "+pattern+"===========>"+ count)
    count
  }

  def grep(pattern: String, fileList: Array[File]): (Int,Int) = {
    var total=0
    var logLines=0
    for (file <-fileList) {
      val source = Source.fromFile(file).getLines().toList
      //    matchLines(source,pattern).foreach(x => println("line " + x._2 + ": " + x._1))
      total+=matchLines(source, pattern, file)
      logLines+=source.length
    }
//    println(pattern+"="+total)
    (total,logLines)
//    source.close
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
object HtmlHelper {
  def prepareAlertTitle()={

  }
  def getAlertsList(exceptionMap: Map[String,(String,String,Int,Int,Int,Int)],StartTime:String,EndTime:String):ListBuffer[String]={
    val htmlAlerts=new ListBuffer[String]()

    for (exception<-exceptionMap) {
      val alertString = new StringBuilder
      alertString.append(s"""<p><strong>Start Time:</strong> ${StartTime}</p>""").append("\n")
      alertString.append(s"""<p><strong>End Time:</strong> ${EndTime}</p>""").append("\n")
      alertString.append(s"""<p><strong>Exception Type:</strong> ${exception._1}</p>""").append("\n")
      alertString.append(s"""<p><strong>Search String: </strong>${exception._2._1}</p>""").append("\n")
      alertString.append(s"""<p><strong>Indicant: </strong>${exception._2._2}</p>""").append("\n")
      alertString.append(s"""<p><strong>Severity: </strong>${exception._2._3}</p>""").append("\n")
      alertString.append(s"""<p><strong>Priority: </strong>${exception._2._4}</p>""").append("\n")
      alertString.append(s"""<p><strong>Occurrences: </strong>${exception._2._5}</p>""").append("\n")
      alertString.append(s"""<p><strong>Total Logs Lines: </strong>${exception._2._6}</p>""").append("\n")
      htmlAlerts+=alertString.toString
      alertString.clear()
//      println(htmlAlerts)
    }
//    println("======>\r\n"+htmlAlerts.size)
//    htmlAlerts.foreach(a=>println(a.toString))
    htmlAlerts
  }
//  val ExceptionHeader=List("Desc","Exeption String","Indicate","Severity","Priority","Occurrences","TotalLines")
  def getNewLine(): String = "</br>"

  def getParagraph(p: String): String = {
    s"<p><strong>$p</strong></p>"
  }
def getHeader(ExceptionHeader:String):String={
  val htmlHeader = new StringBuilder
  htmlHeader.append("""<table style="width:50%" border="1">""").append("\n")
  val headerColumns=ExceptionHeader.split(",")
  for(e<-headerColumns)
    htmlHeader.append(s"""<th width="350" ">${e}</th>""").append("\n")
  htmlHeader.toString
}
  def getHtmlTable(tbl: Map[String,(String,String,Int,Int,Int,Int)],ExceptionHeader:String): String = {
    val htmlTable = new StringBuilder
    htmlTable.append(getHeader(ExceptionHeader))

//      htmlTable.append("""<table style="height: 50px;font-size: 15px;border-collapse: collapse" width="700" border="1">""").append("\n")
    tbl.foreach(row => {
      htmlTable.append("<tr>").append("\n")
//      if (isHeader) {
//        htmlTable.append(s"""<th width="350">${row._1}</th>""").append("\n")
//          .append(s"""<th width="350"">${row._2}</th>""").append("\n")
//        isHeader = false
//      } else {
        val cols=row._2.productIterator.toList
        htmlTable.append(s"""<td width="350" >${row._1}</td>""").append("\n")
        for(e<-cols)
        htmlTable.append(s"""<td width="350" >${e}</td>""").append("\n")

      htmlTable.append("<tr>").append("\n")
    })
    htmlTable.append("</tbody>").append("\n")
      .append("</table>").append("\n")
    htmlTable.toString
  }

  def getHtmlForQueryResult(queryResult: (String, String, List[List[String]])): String = {
    val htmlTable = new StringBuilder()
    val dataDate = queryResult._1
    val tableName = queryResult._2
    val rows = queryResult._3
    val columnsCount = if (rows != null && rows.length != 0 && rows(0).length != 0) rows(0).length else 0
    var sequenceNum = 0
    htmlTable.append("<br/>").append("\n")
      .append("""<table style="height: 50px;font-size: 15px;border-collapse: collapse" width="700" border="1">""").append("\n")
      .append("<tbody>").append("\n")
      .append("<tr>").append("\n")
      .append(s""" <th colspan="${columnsCount + 1}" style='font-size:20px'>${tableName} &nbsp;&nbsp;&nbsp;${dataDate}</th>""")
      .append("</tr>").append("\n")
    rows.foreach(r => {
      htmlTable.append("<tr>").append("\n")
      if (sequenceNum == 0) {
        htmlTable.append(s"""<th style="width: 50px;">No.</th>""").append("\n")
        r.foreach(c => {
          htmlTable.append(s"""<th style="width: 100px;">${c}</th>""").append("\n")
        })
      } else {
        htmlTable.append(s"""<td style="width: 50px;">${sequenceNum}</td>""").append("\n")
        r.foreach(c => {
          htmlTable.append(s"""<td style="width: 100px;">${c}</td>""").append("\n")
        })
      }
      htmlTable.append("<tr>").append("\n")
      sequenceNum += 1
    })
    htmlTable.append("</tbody>").append("\n")
      .append("</table>").append("\n")
    htmlTable.toString
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

  def getLinesFromFile(path: String): List[String] = {
    fromFile(getFile(path)).getLines().toList
  }
  def writeNewFile(fileName: String, reports:ListBuffer[String]): ListBuffer[String] = {
//    val file = getFile(fileName)
    val alertFileList=new ListBuffer[String]
    try {
      var exceptionSeq=0
      for (rep <- reports){
        exceptionSeq+=1
        Log.logMsg("new Alert File"+exceptionSeq+":"+fileName+"/Exception_"+TimeHelper.getCurrentTime_F()+"_"+exceptionSeq+".html")
        val bw = new BufferedWriter(new FileWriter(new File(fileName+"/Exception_"+TimeHelper.getCurrentTime_F().replace(" ","")+"_"+exceptionSeq+".html")))
        alertFileList+=fileName+"/Exception_"+TimeHelper.getCurrentTime_F()+"_"+exceptionSeq+".html"
        //      content.foreach(f => {
        bw.write(rep)
        //bw.newLine
        //      }

        bw.close()
      }
      alertFileList
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

object TimeHelper{
  val fileFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_HHmmss")
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val todayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  def getCurrentTime():String={
    val time = LocalDateTime.now().format(formatter)
    time
  }
  def getCurrentTime_F():String={
    val time = LocalDateTime.now().format(fileFormatter)
    time
  }
  def getCurrentTime_today():String={
    val time = LocalDateTime.now().format(todayFormatter)
    time
  }
}
object EmailSender {

  import sys.process._

  private val WORK_DIR = "/home/daasuser/samer_scripts/SwiftAlert/Email/"
  // private val WORK_DIR = "/mnt/beegfs/Deployment/DEV/Email/Dsl/"

  //private val WORK_DIR = "/mnt/beegfs/tmp/ahmed/email/"

  private val HEADER_FILE = WORK_DIR + "mailheader"
  private val SEND_EMAIL_SCRIPT = WORK_DIR + "sendEmail.sh"
  val dataDt=TimeHelper.getCurrentTime_today()
  val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
  def sendEmail(alertFiles:ListBuffer[String],subject:String,emailType:String): Unit = {
    val headerText = FileHelper.getStringFromFile(HEADER_FILE)
    val mailHeader = headerText.replace("${SUBJECT}", subject)

    val historyEmailDir = WORK_DIR + "History" + FileHelper.FILE_SEPARATOR + dataDt
    FileHelper.mkDir(historyEmailDir)
    val emailHeader = historyEmailDir + FileHelper.FILE_SEPARATOR + "header_" + time
    FileHelper.writeNewFile(emailHeader, List(mailHeader), false)
//
//    val emailBody = historyEmailDir + FileHelper.FILE_SEPARATOR + emailType + "_" + dataDt + "_" + time + ".html"
//    FileHelper.writeNewFile(emailBody, List(email.htmlBody), false)
//
//    val summaryEmailBody = historyEmailDir + FileHelper.FILE_SEPARATOR + emailType + "_" + dataDt + "_summary_" + time + ".html"
//    FileHelper.writeNewFile(summaryEmailBody, List(email.htmlSummaryData), false)

    val summaryEmailBody = historyEmailDir + FileHelper.FILE_SEPARATOR + emailType + "_" + dataDt + "_summary_" + time + ".html"
for(a<-alertFiles) {
//  Seq("ssh", "edge01002", "bash") !;
  Seq("ssh", "edge01002", "bash", SEND_EMAIL_SCRIPT, emailHeader, a) !;
//  println(a)
}
  }

}
SwiftAlert.main(args)
