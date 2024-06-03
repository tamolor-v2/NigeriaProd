#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

  import java.io.{File, _}
  import java.nio.file.{Files, Paths, StandardCopyOption}
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter
  import scala.util.control.Breaks._
  import com.typesafe.config.ConfigFactory
  import sun.rmi.runtime.Log
  import sun.tools.jar.resources.jar
  import Implicits._
  import scala.collection.JavaConversions._
  import scala.collection.mutable.ListBuffer
  import scala.io.Source
  import scala.io.Source.fromFile
  import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

object FeedPipeLineInjector {

case class PipeLineFiles(var ListLiveIncoming:String="", var CreatePartition:String = "", var LineCount:String = "",
                         var ValidationTool:String="", var MoveLiveProcessed:String="", var MiniTar:String="", var DoubleTar:String="")

  case class Feed(seq: Int, name:String,Reg: String,  msisdn: String, ops: String,opps:String,hdfsCheck:String,rg:String,minitar_filename:String)


var pipeLineFiles=new PipeLineFiles()
   var configFile: String =""
// "C:\\Testing\\FeedAdder\\Winadder.conf"
    var ListLiveIncoming = ""
    var CreatePartition = ""
    var LineCount = ""
    var ValidationToolLocation=""
    var MoveLiveProcessed=""
  var MiniTar=""
  var DoubleTar=""
    val NodeAlertMap: scala.collection.mutable.Map[String, ListBuffer[String]] = scala.collection.mutable.Map(
      ("Node Log Removed", new ListBuffer[String]()),
      ("Node Log Added", new ListBuffer[String]())
    )
    val HomeDir="C:\\Testing\\"

    //  val HomeDir="/mnt/beegfs/FlareLoadCluster/SwiftAlert_V2/"
    def main(args: Array[String]): Unit = {
      val time = TimeHelper.getCurrentTime()
      Log.logMsg("Starting")
      Log.logMsg("Parsing args")
          parsArgs(args)
      try {
        //parsArgs(Array("--configFile", "C:\\Testing\\FeedAdder\\Winadder.conf"))
        //parsArgs(Array("--configFile", "/mnt/beegfs/tools/pipeLine/conf/pipLine.conf"))
        Log.logMsg("Successfully Parsed args")
        val feedsList = parseMetadata(configFile)
        Log.logMsg("Successfully prepared Feeds Metadata list ")
        var fileMap=Map[String,ListBuffer[String]]()
        Log.logMsg("preparing to add feed to move live processed")
        val MoveLiveProcessed = addToMoveLiveProcessed(feedsList)
        fileMap+=((pipeLineFiles.MoveLiveProcessed,MoveLiveProcessed))
	println(pipeLineFiles.MoveLiveProcessed)
	//MoveLiveProcessed.foreach(println)
        Log.logMsg("preparing to add feed to line_count")
        val line_count = addToLineCount(feedsList)
        fileMap+=((pipeLineFiles.LineCount,line_count))
        Log.logMsg("preparing to add feed to validation tool")
        //val validationTool = addToValidationTool(feedsList)
        //fileMap+=((pipeLineFiles.ValidationTool,validationTool))
        Log.logMsg("preparing to add feed to list live incoming")
        val listLiveIncoming = addToListLiveIncoming(feedsList)
        fileMap+=((pipeLineFiles.ListLiveIncoming,listLiveIncoming))
        Log.logMsg("preparing to add feed to create partitions")
        val createPartitions = addTocreatePartition(feedsList)
        fileMap+=((pipeLineFiles.CreatePartition,createPartitions))
        Log.logMsg("preparing to add feed to mini tar")
        val miniTar = addToMiniTar(feedsList)
        fileMap+=((pipeLineFiles.MiniTar,miniTar))
        Log.logMsg("preparing to add feed to double tar")
        val doubleTar = addToDoubleTar(feedsList)
        fileMap+=((pipeLineFiles.DoubleTar,doubleTar))
	fileMap-="$outer"
	fileMap.map(a=>println(a._1))
        backupPipeFiles(pipeLineFiles)
        FileHelper.writeFiles(fileMap)
      }
      catch {
        case e: FileNotFoundException => Log.logError("Couldn't find one of the files.",true)
        case e: IOException => Log.logError("Had an IOException trying to read one of the files",true)
      }

      Log.logMsg("Finished")
      System.exit(0)
    }
def writeFiles(files:Map[String,ListBuffer[String]])={

}
def backupPipeFiles(allPipeFiles:PipeLineFiles): Unit ={
  val files=allPipeFiles.toMapWithFields
files.foreach(println)
  for (file<-files){
    Log.logMsg("Backing up file: "+file._1+","+file._2.toString)
    FileHelper.backupFile(file._1,file._2.toString)
  }
}

  def addToDoubleTar(feeds_list:ListBuffer[Feed]):ListBuffer[String] ={
    var finalScriptLines=scala.collection.mutable.ListBuffer[String]()
    val file_content=FileHelper.getLinesFromFile(DoubleTar)
    finalScriptLines=file_content
    for (feed <-feeds_list){
      finalScriptLines=finalScriptLines.map(
        a=> if (a.startsWith("time java") && a.contains("-vad -rmp") && a.contains("-fromHdfs"))
          a.replace("""/$date -nt""","""/$date $hdfs/"""+feed.name +"/$date -nt" )
        else a)

        finalScriptLines = finalScriptLines.map(
          a => if (a.startsWith("time java") && a.contains("-cvzf") && a.contains("""AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA"""))
            a.replace("""$date -outarchive""","""$date  $infolder/""" + feed.name +"""/$date -outarchive """).replace(""")-*.([0-9]{8})""","""|""" + feed.name +""")-*.([0-9]{8})""")
          else a)

    }
    //finalScriptLines.foreach(println)
    finalScriptLines
  }

  def addToMiniTar(feeds_list:ListBuffer[Feed]):ListBuffer[String] ={
    var finalScriptLines=scala.collection.mutable.ListBuffer[String]()
    val file_content=FileHelper.getLinesFromFile(MiniTar)
    finalScriptLines=file_content
    for (feed <-feeds_list){
      finalScriptLines=finalScriptLines.map(
        a=> if (a.startsWith("time java") && a.contains("-vad -rmp") && a.contains("$date/$date -nt"))
        a.replace("""$date/$date -nt""","""$date/$date $infolder/"""+feed.name +"/$date/$date -nt" )

      else a)
      if (feed.minitar_filename.equalsIgnoreCase("standard")) {
        finalScriptLines = finalScriptLines.map(
          a => if (a.startsWith("time java") && a.contains("-cvzf") && a.contains("""AIR_ADJ_DA|AIR_ADJ_MA|AIR_REFILL_AC|AIR_REFILL_DA"""))
            a.replace("""$date -outarchive""","""$date $infolder/""" + feed.name +"""/$date""").replace(""")_.*$""","""|""" + feed.name +""")_.*$""")
          else a)
      } else if (!feed.minitar_filename.equalsIgnoreCase("standard")){
        finalScriptLines+="#delete "+feed.name
        finalScriptLines+="""mytime=$(date +"%Y-%m-%d_%H-%M-%S")"""
        finalScriptLines+="""time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml  /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in $infolder/"""+feed.name+"""/$date/$date  -nt 32 -vad -rmp $infolder -out $report_folder -dp 15 -tf 1 -nosim -lbl mini_delete_ -of -od -fnfp 4"""
        finalScriptLines+=""
        finalScriptLines+="#"+feed.name
        finalScriptLines+= """time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar  -in  $infolder/"""+feed.name+"""/$date  -outarchive "$infolder/#3/#1/#1" -nt 32 -cvzf -tarPrefix "#3-#1-#2" -rgrm "$infolder/#3" -groupregex"""+feed.rg+""" -out $report_folder -dp 15 -tf 1 -ts 80 -effl "Archive-*.*$" -iffl "^([0-9]{8})([0-9]{2}).*_(SDP)_.*$" -nosim -lbl mini_taring_  2>&1 | tee "$report_folder/mini_taring_$mytime.log"""".stripMargin
        finalScriptLines+=""
        finalScriptLines+="#delete "+feed.name
        finalScriptLines+="""time java -Xmx50g -Xms50g -jar -Dlog4j.configurationFile=/mnt/beegfs/production/tarringscript/log4j2.xml  /mnt/beegfs/production/tarringscript/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in $infolder/"""+feed.name+"""/$date/$date  -nt 32 -vad -rmp $infolder -out $report_folder -dp 15 -tf 1 -nosim -lbl mini_delete_ -of -od -fnfp 4"""
      }
    }
//        finalScriptLines.foreach(println)
    finalScriptLines
  }


 def addToMoveLiveProcessed(feeds_list:ListBuffer[Feed]):ListBuffer[String] = {
    var finalScriptLines = scala.collection.mutable.ListBuffer[String]()
    var tempScriptLines = scala.collection.mutable.ListBuffer[String]()
    var file_content = FileHelper.getLinesFromFile(MoveLiveProcessed)
    finalScriptLines=file_content
    for (feed <- feeds_list) {
      println("Ops ====>"+feed.ops)
      println("OPPS====>"+feed.opps)
      for (l <- finalScriptLines) {
        if (l =="""#add new feeds""") {
          tempScriptLines +=""
          tempScriptLines +="#"+feed.name
          tempScriptLines +="#------------------"
          tempScriptLines +=
            """mytime=$(date +"%Y-%m-%d_%H-%M-%S")"""
          tempScriptLines +="""time java -Xmx30g -Xms30g -Dlog4j.configurationFile=/mnt/beegfs/tools/fileOps/log4j2.xml -jar /mnt/beegfs/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar -in /mnt/beegfs/production/live/""" +
            feed.ops +
            """/processed -out /mnt/beegfs/tools/Crontab/logs/list/${currDate} -od -tf 10 -nt 32 -dp 15 -of -op list -mv /mnt/beegfs/production/archived/""" + feed.ops +
            """/ -re -rg """ + "\"" + feed.rg + "\"" +
            """ -opp """+"\""+feed.opps+"\""+"""  -nosim -lbl move_live_proces_arch_"""+feed.ops+""" 2>&1  |tee "/mnt/beegfs/tools/Crontab/logs/move/${currDate}/move_live_proces_arch_"""+
            feed.ops+"""_$mytime.log""""

          tempScriptLines +=""
          tempScriptLines +="""#add new feeds"""
        }
        else {
          tempScriptLines += l
        }
      }
      //      println("1-"+tempScriptLines)
      finalScriptLines=tempScriptLines.map {x => x}
      tempScriptLines.clear()
      //      println("2-"+tempScriptLines)
    }
    //    finalScriptLines.foreach(println)
    finalScriptLines
  }

  def addTocreatePartition(feeds_list:ListBuffer[Feed]):ListBuffer[String] ={
    var finalScriptLines=scala.collection.mutable.ListBuffer[String]()
    val file_content=FileHelper.getLinesFromFile(CreatePartition)
    finalScriptLines=file_content
    for (feed <-feeds_list){
      finalScriptLines=finalScriptLines.map( a=> if (a.startsWith("for feed in"))
        a+" "+feed.name
      else a)
    }

    finalScriptLines
  }

  def addToListLiveIncoming(feeds_list:ListBuffer[Feed]):ListBuffer[String] ={
    var finalScriptLines=scala.collection.mutable.ListBuffer[String]()
    val file_content=FileHelper.getLinesFromFile(ListLiveIncoming)
    finalScriptLines=file_content
    for (feed <-feeds_list){
      finalScriptLines=finalScriptLines.map( a=> if (a.startsWith("time java"))
        a.replace("""/incoming -out ""","""/incoming /mnt/beegfs/live/"""+feed.name +"/incoming -out " )
      else a)
    }
//    finalScriptLines.foreach(println)
    finalScriptLines
  }

  def addToValidationTool(feeds_list:ListBuffer[Feed]):ListBuffer[String] ={
    var finalScriptLines=scala.collection.mutable.ListBuffer[String]()

    val file_content=FileHelper.getLinesFromFile(ValidationToolLocation)
    finalScriptLines=file_content
    for (feed <-feeds_list){
      finalScriptLines=finalScriptLines.map( a=> if (a.startsWith("Feeds="))
        a.dropRight(1) +" "+feed.name +"\""
      else a)
      finalScriptLines += feed.name +"_REG="+feed.Reg
      finalScriptLines += feed.name +"_OPS="+feed.ops
      finalScriptLines += feed.name +"_IgnoreHDFSCheck="+feed.hdfsCheck
    }
    finalScriptLines
  }

 def addToLineCount(feeds_list:ListBuffer[Feed]):ListBuffer[String]={
   var finalScriptLines=scala.collection.mutable.ListBuffer[String]()
   val file_content=FileHelper.getLinesFromFile(LineCount)
   finalScriptLines=file_content
   for (feed <-feeds_list){
     finalScriptLines=finalScriptLines.map( a=> if (a.startsWith("time java"))
       a.replace("""$yest -out ""","$yest /mnt/beegfs/production/archived/"+feed.ops +"/$yest -out " )
     else a)
   }
   finalScriptLines
 }

    def checkPidFile(pidFile:String): Boolean={
      new File (pidFile).exists()
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
      var FeedMetadata = new ListBuffer[Feed]()
      val config = ConfigFactory.parseFile(new File(path)).getConfig("FeedAddaer.LoadCluster")
      val defaultConfig = ConfigFactory.load(config)
      val clusterName = config.getString("ClusterName")
      LineCount = config.getString("LineCount")
      CreatePartition = config.getString("CreatePartition")
      ListLiveIncoming = config.getString("ListLiveIncoming")
      ValidationToolLocation=config.getString("ValidationToolLocation")
      MoveLiveProcessed=config.getString("MoveLiveProcessed")
      MiniTar=config.getString("MiniTar")
      DoubleTar=config.getString("DoubleTar")
      pipeLineFiles=PipeLineFiles(ListLiveIncoming,CreatePartition,LineCount,ValidationToolLocation,MoveLiveProcessed,
        MiniTar,DoubleTar)
      val feedsList=config.getObject("Feeds").entrySet().toList.map(entry => (entry.getKey, entry.getValue.unwrapped())).toMap.toSeq.sortBy(_._1)
      for(feed<-feedsList)
        {
          val metadata = feed._2.toString.drop(1).dropRight(1)
          FeedMetadata +=  processfeeds(metadata)
        }
      FeedMetadata
    }

    def processfeeds( metadata: String): Feed = {
      val result = metadata.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)").map(a => a.trim)
      var seq = 0
      var name = ""
      var reg = ""
      var msisdn = ""
      var ops = ""
      var opps=""
      var hdfsCheck=""
      var rg=""
      var minitar_filename=""
      for (a <- result) {
        val kv = a.split("=")
        if (kv(0).toLowerCase == "seq")
          seq = kv(1).toInt
        else if (kv(0).toLowerCase == "name")
          name = kv(1)
        else if (kv(0).toLowerCase == "reg")
          reg = kv(1).replaceAll("\\|",",")
        else if (kv(0).toLowerCase == "msisdnkey")
          msisdn = kv(1)
        else if (kv(0).toLowerCase == "opps") {
          opps = kv(1)
        }
        else if (kv(0).toLowerCase == "ops") {
          ops = kv(1)
        }
        else if (kv(0).toLowerCase == "ignorehdfscheck") {
          hdfsCheck = kv(1)
        }
        else if (kv(0).toLowerCase == "mv_lv_pr_regex") {
          rg = kv(1)
        }
        else if (kv(0).toLowerCase == "minitar_filename") {
          minitar_filename = kv(1)
        }
      }
	println("opps =====>"+opps)
	println("ops =====>"+ops)
      return Feed( seq, name, reg,msisdn,ops,opps,hdfsCheck,rg,minitar_filename)
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
  object FileHelper {
    val FILE_SEPARATOR: String = System.getProperty("file.separator")

    private def getFile(path: String): File = {
      if (path == null || path.length == 0) {
        throw new IllegalArgumentException(s"Path is not valid ($path)")
      }
      new File(path)
    }


    def writeFiles(files:Map[String,ListBuffer[String]])={
      for(file<-files){
//      val bw = new BufferedWriter(new FileWriter(new File(file._1)))
        Log.logMsg("Writing new file:" +file._1)
        writeListBuffer(file._1,file._2)
      }
    }
    def writeListBuffer(fileName:String,lines:ListBuffer[String]): Unit ={
println("Writing "+fileName)
      val bw = new BufferedWriter(new FileWriter(new File(fileName)))
      try{
      for (line<-lines){
        bw.write(line+"\n")
      }
      }catch{
        case e:Exception=>e.printStackTrace
      }
      finally {bw.close()}
    }

def backupFile(dir:String,file:String): Unit ={
  val timeStampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val runTimeStamp= LocalDateTime.now.format(timeStampFormatter)
println(dir+":"+file)

if(dir!="$outer" && dir!="ValidationTool"){	
  val file_name=file.substring(file.lastIndexOf("/")+1,file.length)
  val bkPath=file.substring(0,file.lastIndexOf("/"))+"/scriptBackup"+"/"+dir
  try {mkDir(bkPath)
  }catch {
    case e:Exception=>e.printStackTrace()
  }
try{copyFile(file,bkPath+"/"+runTimeStamp+"_"+file_name)
}catch{
  case e:Exception=>e.printStackTrace()
}
}
}
    def copyFile(src:String,dest:String): Unit ={
      val inputChannel = new FileInputStream(src).getChannel
      val outputChannel = new FileOutputStream(dest).getChannel
      outputChannel.transferFrom(inputChannel, 0, inputChannel.size)
      inputChannel.close

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

    def getLinesFromFile(path: String): ListBuffer[String] = {

      val file = getFile(path)
      if (!file.exists())
        throw new FileNotFoundException(file.getAbsolutePath)

      fromFile(file).getLines().to[ListBuffer]


    }
    def recursiveListFiles(f: File): Array[File] = {
      val currentDir = f.listFiles
      currentDir ++ currentDir.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

//    def getLinesFromFile(path: String): List[String] = {
//      fromFile(getFile(path)).getLines().toList
//    }

    def writeNewFiles(fileName: String, reports: ListBuffer[String]): ListBuffer[String] = {
      //    println(history_LineCont+"\n"+reports)
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
          bw.write(a._1 + "," + a._2 + "\n")
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
    def writeInTableCellLineBreaks(k:String,v:String):String={
      var str=s"""<tr><td><p><strong>${k}</td><td>"""
      val lines=v.split(",")
      val LineSet=lines.toList
      //    println("=====>"+LineSet.size)
      //    println((lines.toList))
      for(l<-lines)
        str+=s"""${l}<br>"""

      return s"""${str}</td></tr>\n"""
    }

    private def _summary(s: Seq[Any]): Map[String, Int] = {
      val tuples: Seq[(String, Any)] =
        s.map { k =>
          k match {
            case s: String => (s, s)
            case i: Int    => ("integer", i)
          }
        }
      val groups: Map[String, Seq[Any]] = tuples.groupBy(_._1).mapValues(_.map( _._2 ))
      val counts: Map[String, Int] = groups.map{ case (k,v) => (k, v.length) }
      counts
    }

    def getAlertsList(exceptionMap: scala.collection.mutable.Map[String,(String,String,String,String,Int,Int,Int,Int,String,ListBuffer[Int],ListBuffer[String],
      ListBuffer[String])],StartTime:String,EndTime:String,TotalExeptionLines:Int,totalNodesCount:Int):ListBuffer[String]={
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
        //      println(exception._2._1)
        if(exception._2._1.equalsIgnoreCase("exception") && !exception._2._3.equals("ERROR") ) {
          //        exception._2._11.foreach(println)
          alertString.append(writeInTableCell("Extracted Entitities", exception._2._11.mkString(",").replace("  "," "))).append("\n")
        }else if(exception._2._1.equalsIgnoreCase("exception") && exception._2._3.equals("ERROR") ) {
          alertString.append(writeInTableCellLineBreaks("Extracted Entitities", exception._2._11.mkString(","))).append("\n")
          //        println(exception._2._11.mkString("\n"))
        }
        //      if(exception._2._1.equalsIgnoreCase("Exception"))
        alertString.append(writeInTableCell("Affected Log Files ",exception._2._12.mkString(","))).append("\n")
        //      else
        //        alertString.append(s"""<p><strong>Nodes Not performing task: </strong>${exception._2._12.mkString(", ")}</p>""").append("\n")
        alertString.append(writeInTableCell("Total Occurrences in Log files",exception._2._7.toString)).append("\n")
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
    private val HEADER_FILE = WORK_DIR + "mailheader_samer"
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

  }
  FeedPipeLineInjector.main(args)




