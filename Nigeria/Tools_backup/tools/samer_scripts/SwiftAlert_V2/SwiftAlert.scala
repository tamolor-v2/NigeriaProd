#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

import java.io.{File, _}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
//import shapeless.ops.nat.GT.>
//import sun.rmi.runtime.Log

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.io.Source.fromFile
import scala.util.matching.Regex


case class KamLogException(clusterName: String, logPtah: String, seq: Int, ExceptionType: String, alertType: String, searchString: String, msg: String, severity: Int,
                           priority: Int, repetition: Int = 0, totalLogLines: Int = 0, extractRegEx: String = "", extractGroup: ListBuffer[Int] =  ListBuffer[Int](),
                           ExtractedEntities: ListBuffer[String] =  ListBuffer[String](), Nodes: ListBuffer[String] =  ListBuffer[String]())

//case class Email(subject: String, htmlBody: String, htmlSummaryData: String)
//case class historyStats(filePath:String,)
object SwiftAlert {
  var configFile: String = null
  var headerColumns = ""
  var outputDir = ""
  //  var startLine=0
  var statsFile = ""
  var logPath = ""
  var totalExceptionCount=0
  val NodeAlertMap: scala.collection.mutable.Map[String, ListBuffer[String]] = scala.collection.mutable.Map(
    ("Node Log Removed", new ListBuffer[String]()),
    ("Node Log Added", new ListBuffer[String]())
  )


  def main(args: Array[String]): Unit = {
    val time = TimeHelper.getCurrentTime()
    Log.logMsg("Starting")
        Log.logMsg("Parsing args")
    parsArgs(args)
                Log.logMsg("Successfully Parsed args")
val exceptionList = parseMetadata(configFile)
        Log.logMsg("Successfully prepared Exception metadata list ")
    val LogFilesList = FileHelper.recursiveListFiles(new java.io.File(logPath)).toList
                Log.logMsg("Successfully prepared Log file List ")
    val filesHistoryLineCount: scala.collection.mutable.Map[String, Int] = getfilesHistoryLineCount()
	                Log.logMsg("Retrieved Last run log files Stats")
    val k = isLogHistory(filesHistoryLineCount)
	                Log.logMsg("searching for changes in log files")
    processLogFilesChangs(LogFilesList, exceptionList, filesHistoryLineCount)
        val (finalList,finalMapHistory)=processExceptionList(exceptionList,filesHistoryLineCount)
				                Log.logMsg("Processed Exception List")

        val final_tuple=finalList flatMap KamLogException.unapply
        val finalMap=final_tuple.groupBy(rc=>rc._1+"-"+rc._3).map { case (k,v) => (k, (v(0)._4,v(0)._5,v(0)._6,v(0)._7,v(0)._8,v(0)._9,v(0)._10,v(0)._11,v(0)._12,v(0)._13,v(0)._14,v(0)._15))}
				                Log.logMsg("Cleaning Exceptions stats")
        val fMap=cleanExceptions(finalMap,LogFilesList.map(a=>a.getPath))
//    for(a<-fMap)
//      println(a)
				                Log.logMsg("Writing alerts to files")
    FileHelper.writeMap(finalMapHistory, statsFile)
        if(fMap.size>0) {
						                Log.logMsg("Preparing Alerts HTML Emails")
          val alertList = HtmlHelper.getAlertsList(fMap, time, TimeHelper.getCurrentTime(),totalExceptionCount)
          val alertFiles = FileHelper.writeNewFiles(outputDir, alertList)
		  				                Log.logMsg("Preparing to send Emails")
          EmailSender.sendEmail(alertFiles,"Kamanja Swift Alert_test","LogExceptions")
System.exit(0)
        }
        else
          Log.logMsg("No Alerts were generated")
    Log.logMsg("Finished")
System.exit(0)
  }

    def cleanExceptions(ExceptionFinalReport:Map[String,(String,String,String,String,Int,Int,Int,Int,String,ListBuffer[Int],ListBuffer[String],ListBuffer[String])],filesList: List[String]):
    scala.collection.mutable.Map[String,(String,String,String,String,Int,Int,Int,Int,String,ListBuffer[Int],ListBuffer[String],ListBuffer[String])]={
      var ExceptionFinalRepeated = ExceptionFinalReport.filter(_._2._7>0  )
      var finalExceptionMap=   scala.collection.mutable.Map[String,(String,String,String,String,Int,Int,Int,Int,String,ListBuffer[Int],ListBuffer[String],ListBuffer[String])]()

      for(a<-ExceptionFinalRepeated){

        if(a._2._1.equalsIgnoreCase("Persistant")  ) {
          val persistantStrings=filesList.map(f=>f.substring(f.indexOf("Node"), f.lastIndexOf("."))).filterNot(_.isEmpty).
            diff(a._2._12.toList).to[ListBuffer]
//filesList.map(f=>f.substring(f.indexOf("Node"), f.lastIndexOf("."))).filterNot(_.isEmpty).foreach(println)
//          val persistantStrings = filesList.map(f => f.substring(f.indexOf("Node"), f.lastIndexOf("."))).filterNot(_.isEmpty)
//          if (persistantStrings.length == 0) {
            finalExceptionMap(a._1 + "_" + a._2._2) = ((a._2._1, a._2._2, a._2._3, a._2._4, a._2._5, a._2._6, a._2._7, a._2._8, a._2._9, a._2._10, a._2._11,persistantStrings))
//          } else{
//            finalExceptionMap(a._1+"_"+a._2._2)=((a._2._1,a._2._2,a._2._3,a._2._4,a._2._5,a._2._6,a._2._7,a._2._8,a._2._9,a._2._10,a._2._11,new ListBuffer[String]))
          }

          else if(a._2._1.equalsIgnoreCase("Exception")){
            finalExceptionMap(a._1+"_"+a._2._2)=((a._2._1,a._2._2,a._2._3,a._2._4,a._2._5,a._2._6,a._2._7,a._2._8,a._2._9,a._2._10,a._2._11,a._2._12))
          }
      }
      finalExceptionMap.filterNot(_._2._12.isEmpty  )
    }

  def processLogFilesChangs(filesList: List[File], ExceptionList: ListBuffer[KamLogException], filesHistoryLineCount: scala.collection.mutable.Map[String, Int]) = {
    val logFilesDiff = compareLogFilesNamesAndHistory(filesList.map(a => a.getPath), filesHistoryLineCount)
    val l = logFilesDiff.map(a => a.substring(a.indexOf("Node"), a.lastIndexOf("."))).to[ListBuffer]
    if (filesList.length > filesHistoryLineCount.size) {

      NodeAlertMap("Node Added") = l
      for (node <- logFilesDiff)
        Log.logMsg(s"A log file has been added for ${node.substring(node.indexOf("Node"), node.lastIndexOf("."))} ")

    } else {

      NodeAlertMap("Node Removed") = l
      for (node <- logFilesDiff)
        Log.logMsg(s"A log file has been removed for ${node.substring(node.indexOf("Node"), node.lastIndexOf("."))}")
    }
  }

  def compareLogFilesNamesAndHistory(filesList: List[String], filesHistoryLineCount: scala.collection.mutable.Map[String, Int]): List[String] = {
    val LogFilesDiff = if (filesList.length >= filesHistoryLineCount.size) filesList.diff(filesHistoryLineCount.map(a => a._1).toList) else filesHistoryLineCount.map(a => a._1).toList.diff(filesList)
    LogFilesDiff
  }

  def isLogHistory(HistoryLogFiles: scala.collection.mutable.Map[String, Int]): Boolean = {
    val historyFilesLinesList = HistoryLogFiles.filter(a => a._2 > 0)
    if (historyFilesLinesList.size > 0) {
      Log.logMsg("History log stats exists")
      true
    }
    else {
      Log.logMsg("History log stats doesn't exist")
      false
    }
  }

  def getfilesHistoryLineCount(): scala.collection.mutable.Map[String, Int] = {
    val filesCountMap = scala.collection.mutable.Map[String, Int]()
    val logFilesList = FileHelper.recursiveListFiles(new java.io.File(logPath))
    if (!Files.exists(Paths.get(statsFile))) {
      for (logF <- logFilesList) {
        filesCountMap += (logF.toString -> 0)
      }
      return filesCountMap
    }
    else {
      val fList = FileHelper.getLinesFromFile(statsFile)
      if (fList.length == 0) {
        for (logF <- logFilesList) {
          filesCountMap += (logF.toString -> 0)
        }
        return filesCountMap
      }
      else {
        for (l <- fList) {

          filesCountMap += (l.split(",")(0) -> l.split(",")(1).toInt)
        }
        return filesCountMap
      }
    }
  }

  def parsArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--configFile", argDataDate: String) => configFile = argDataDate
      }
    else
      Log.logError("No config file is supplied", true)
  }

  def parseMetadata(path: String) = {
    var ExceptionFinalReport = new ListBuffer[KamLogException]()
    val config = ConfigFactory.parseFile(new File(path)).getConfig("Clusters.LoadCluster")
    val defaultConfig = ConfigFactory.load(config)
    val clusterName = config.getString("ClusterName")
    logPath = config.getString("logsPath")
    outputDir = config.getString("outputDir")
    headerColumns = config.getString("HeaderColumns")
    val dfsExs2 = config.getObject("Exceptions").entrySet().toList.map(entry => (entry.getKey, entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
    for (logException <- dfsExs2) {
      val exceptionType = logException._1
      val metadata = logException._2.toString.drop(1).dropRight(1)
      //.split(",")
      val latestPath = config.getString("latestDir")
      val linesReadFile = config.getString("linesReadFile")
      statsFile = latestPath + linesReadFile
      ExceptionFinalReport += processExceptions(clusterName, logPath, exceptionType, metadata)
    }
    ExceptionFinalReport
  }

  def processExceptions(clusterName: String, logPath: String, exceptionType: String, metadata: String): KamLogException = {
    val result = metadata.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(a => a.trim)
    var msg = ""
    var searchString = ""
    var severity = 0
    var priority = 0
    val totalLogLines = 0
    var repetion = 0
    var statusType = "Exception"
    var extractRegEx = ""
    var extractgroup = ListBuffer[Int]()
    var seq = 0
    var alertType = ""
    for (a <- result) {
      val kv = a.split("=")
      if (kv(0).toLowerCase == "msg")
        msg = kv(1)
      else if (kv(0).toLowerCase.contains("searchstring")) {
        searchString = kv(1)
      }
      else if (kv(0).toLowerCase == "severity")
        severity = kv(1).toInt
      else if (kv(0).toLowerCase == "priority")
        priority = kv(1).toInt
      else if (kv(0).toLowerCase == "statustype")
        statusType = kv(1)
      else if (kv(0).toLowerCase == "extractregex") {
        //        if( kv(1))
        extractRegEx = kv(1)
      }
      else if (kv(0).toLowerCase == "extractgroup") {
        val extr=kv(1).split("\\|")
        extractgroup = extr.map(_.toInt).to[ListBuffer]
      }
      else if (kv(0).toLowerCase == "alerttype")
        alertType = kv(1)
      else if (kv(0).toLowerCase == "seq")
        seq = kv(1).toInt
    }
    return KamLogException(clusterName, logPath, seq, alertType, exceptionType, searchString, msg, severity, priority, repetion, totalLogLines, extractRegEx, extractgroup)
  }

  def processExceptionList(exceptionList: ListBuffer[KamLogException], mapFilesLinesHistory: scala.collection.mutable.Map[String, Int]):
  (ListBuffer[KamLogException], scala.collection.mutable.Map[String, Int]) = {
    var ExceptionFinalReport = new ListBuffer[KamLogException]()
    val fileLocation = exceptionList.head.logPtah
    var fileList = FileHelper.recursiveListFiles(new File(fileLocation))
    var filesLinesHistory = scala.collection.mutable.Map[String, Int]() ++ mapFilesLinesHistory
    for (a <- exceptionList) {
      var totalLines = 0
      if (a.logPtah != fileLocation)
        fileList = FileHelper.recursiveListFiles(new File(fileLocation))
              var (count,filesLinesHistoryF,totalLogLines,listOfNodes,extractedList)=grep(a.searchString,fileList,mapFilesLinesHistory,a.extractRegEx.r,a.extractGroup)
              totalLines+=count

      if(a.ExceptionType.equalsIgnoreCase("exception") && count>0)
        totalExceptionCount+= count
              if(count<=0)
                ExceptionFinalReport+=a.copy(totalLogLines = totalLogLines,repetition =0)
              else
                ExceptionFinalReport+=a.copy(totalLogLines =totalLogLines,repetition =a.repetition+count,
                  Nodes = listOfNodes.map(a => a.substring(a.indexOf("Node"), a.lastIndexOf("."))).to[ListBuffer],ExtractedEntities=extractedList.to[ListBuffer])
              filesLinesHistory=filesLinesHistoryF
    }
//ExceptionFinalReport.foreach(println)
    (ExceptionFinalReport, filesLinesHistory)
}

  def grep(searchString: String, fileList: Array[File], mapFilesLinesHistory:scala.collection.mutable.Map[String, Int], extractPattern:Regex, location:ListBuffer[Int]):
  (Int,scala.collection.mutable.Map[String, Int],Int,Set[String],List[String]) = {
    var filesContainingString= Set[String]()
    var count=0
    var logLines=0
    var totalExceptionLines=0
    var extractedNotes= Set[String]()
    var localMap=scala.collection.mutable.Map[String, Int]() ++mapFilesLinesHistory
try{
    for (file <-fileList) {
      val filePath=file.toString
      if (mapFilesLinesHistory.contains(filePath)) {
        val startSource = Source.fromFile(file).getLines().toList
        val sourceLengthDiff=startSource.length-mapFilesLinesHistory(filePath)
        logLines+=sourceLengthDiff+mapFilesLinesHistory (filePath)
        val remaining=startSource.length-mapFilesLinesHistory(filePath)
//        println(startSource.length+"=====>"+mapFilesLinesHistory(filePath))
        val source=if(startSource.length>=mapFilesLinesHistory(filePath)) startSource.drop(mapFilesLinesHistory(filePath)) else startSource.drop(0)
        if(source.length>0) {
         val (count,extractedNote) = matchLines(source, searchString ,extractPattern,location)
          if(count>0)
            filesContainingString+=filePath
          if(extractedNote.size>0) {
           // println(extractedNote)
            extractedNotes += extractedNote.mkString
          }

          totalExceptionLines+=count
          localMap(filePath) = startSource.length//+mapFilesLinesHistory(filePath)
        }
      }
    }
    (totalExceptionLines,localMap,logLines,filesContainingString,extractedNotes.toList)
}catch{
      case e: Exception => {
//EmailSender.sendCriticalException("Kamanja Swift Alert_DFS","LogExceptions")
(totalExceptionLines,localMap,logLines,filesContainingString,extractedNotes.toList)
}
}
(totalExceptionLines,localMap,logLines,filesContainingString,extractedNotes.toList)

  }

  def matchLines(source: List[String], searchString: String, extractPattern:Regex, locations:ListBuffer[Int]):(Int,Set[String]) = {
var extractedNote=Set[String]()
    var count=0
    for(l <-source) {
      if (l.contains(searchString)) {
        count += 1
        if(extractPattern!=null ) {
          val res = extractPattern.findAllMatchIn(l).toList
          for(loc<-locations) {
//            if(searchString.equalsIgnoreCase("records took"))
//              println(l)
            extractedNote += res.map(_.group(loc)).headOption.getOrElse("")
          }
        }
      }
    }
    (count,extractedNote.filterNot(_.isEmpty))
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

  def writeNewFiles(fileName: String, reports: ListBuffer[String]): ListBuffer[String] = {
    //    println(history_LineCont+"\r\n"+reports)
    //    val file = getFile(fileName)

    //    bw.close()
//    writeMap(history_LineCont, statsFile)
    val alertFileList = new ListBuffer[String]
    try {
      var exceptionSeq = 0
      for (rep <- reports) {

  val currentdt=TimeHelper.getCurrentTime_F()
        exceptionSeq += 1
        Log.logMsg("new Alert File" + exceptionSeq + ":" + fileName + FileHelper.FILE_SEPARATOR + "Exception_" + currentdt + "_" + exceptionSeq + ".html")
        val bw = new BufferedWriter(new FileWriter(new File(fileName + FileHelper.FILE_SEPARATOR + "Exception_" + currentdt.replace(" ", "") + "_" + exceptionSeq + ".html")))
        alertFileList += fileName + FileHelper.FILE_SEPARATOR + "Exception_" + currentdt+ "_" + exceptionSeq + ".html"


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
        //bw.newLine
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
object HtmlHelper {
  def prepareAlertTitle()={

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

  def getFooter():String={
    val htmlFooter = new StringBuilder
    htmlFooter.append("</table>").append("\n").append("</body>").append("\n")

    htmlFooter.toString
  }
  def writeInTableCell(k:String,v:String):String={
    return s"""<tr><td><p><strong>${k}</td><td> ${v}</td></tr>\n"""
  }
  def getAlertsList(exceptionMap: scala.collection.mutable.Map[String,(String,String,String,String,Int,Int,Int,Int,String,ListBuffer[Int],ListBuffer[String],
    ListBuffer[String])],StartTime:String,EndTime:String,TotalExeptionLines:Int):ListBuffer[String]={
    val htmlAlerts=new ListBuffer[String]()

    for (exception<-exceptionMap) {
      val alertString = new StringBuilder
      alertString.append(getHeader("Kamanja Logs Alert"))
      alertString.append(writeInTableCell("Start Time",StartTime)).append("\n")
      alertString.append(writeInTableCell("End Time",EndTime)).append("\n")
      alertString.append(writeInTableCell("Exception Type",exception._1)).append("\n")
      alertString.append(writeInTableCell("Search String",exception._2._3)).append("\n")
      alertString.append(writeInTableCell("Indicant",exception._2._4)).append("\n")
      alertString.append(writeInTableCell("Severity",exception._2._5.toString)).append("\n")
      alertString.append(writeInTableCell("Priority",exception._2._6.toString)).append("\n")
      if(exception._2._1.equalsIgnoreCase("exception"))
      alertString.append(writeInTableCell("Extracted Entitities",exception._2._11.mkString(", "))).append("\n")
//      if(exception._2._1.equalsIgnoreCase("Exception"))
      alertString.append(writeInTableCell("Affected Node(s)",exception._2._12.mkString(", "))).append("\n")
//      else
//        alertString.append(s"""<p><strong>Nodes Not performing task: </strong>${exception._2._12.mkString(", ")}</p>""").append("\n")
      alertString.append(writeInTableCell("Total Occurrences in Nodes",exception._2._7.toString)).append("\n")
      if(exception._2._1.equalsIgnoreCase("exception"))
      alertString.append(writeInTableCell("Total Exceptions Count",TotalExeptionLines.toString)).append("\n")
      alertString.append(writeInTableCell("Total Logs Lines",exception._2._8.toString)).append("\n")
      alertString.append(getFooter())
      htmlAlerts+=alertString.toString
//      println(alertString)
      alertString.clear()
    }

    htmlAlerts
  }
  //  val ExceptionHeader=List("Desc","Exeption String","Indicate","Severity","Priority","Occurrences","TotalLines")
  def getNewLine(): String = "</br>"

  def getParagraph(p: String): String = {
    s"<p><strong>$p</strong></p>"
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

object EmailSender {
  import sys.process._
  private val WORK_DIR = "/mnt/beegfs/FlareLoadCluster/SwiftAlert_V2/Email/"
  private val HEADER_FILE = WORK_DIR + "mailheader"
  private val SEND_EMAIL_SCRIPT = WORK_DIR + "sendEmail.sh"

  def sendEmail(alertFiles:ListBuffer[String],subject:String,emailType:String): Unit = {
       val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
        val headerText = FileHelper.getStringFromFile(HEADER_FILE)
        val mailHeader = headerText.replace("${SUBJECT}", subject+"_"+time)
        val dataDt=TimeHelper.getCurrentTime_today()
        val historyEmailDir = WORK_DIR + "History" + FileHelper.FILE_SEPARATOR + dataDt
        FileHelper.mkDir(historyEmailDir)
        val emailHeader = historyEmailDir + FileHelper.FILE_SEPARATOR + "header_" + time
        FileHelper.writeNewFile(emailHeader, List(mailHeader), false)
        val summaryEmailBody = historyEmailDir + FileHelper.FILE_SEPARATOR + emailType + "_" + dataDt + "_summary_" + time + ".html"
		var seq=0
    for(a<-alertFiles) {
	seq+=1
					                Log.logMsg("Sending Email Seq_"+seq)
        Seq("ssh", "edge01002", "bash", SEND_EMAIL_SCRIPT, emailHeader, a) !;
    }
  }
  def sendCriticalException(subject:String,emailType:String): Unit = {
    val headerText = FileHelper.getStringFromFile(HEADER_FILE)
    val mailHeader = headerText.replace("${SUBJECT}", subject)
    val dataDt=TimeHelper.getCurrentTime_today()
    val time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"))
    val historyEmailDir = WORK_DIR + "History" + FileHelper.FILE_SEPARATOR + dataDt
    FileHelper.mkDir(historyEmailDir)
    val emailHeader = historyEmailDir + FileHelper.FILE_SEPARATOR + "header_" + time
    FileHelper.writeNewFile(emailHeader, List(mailHeader), false)
    val summaryEmailBody = historyEmailDir + FileHelper.FILE_SEPARATOR + emailType + "_" + dataDt + "_summary_" + time + ".html"
      Seq("ssh", "edge01002", "bash", SEND_EMAIL_SCRIPT, emailHeader) !;
  }

}
SwiftAlert.main(args)
System.exit(0)


