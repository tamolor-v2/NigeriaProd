#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

import java.io.{File, _}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import scala.sys.process.ProcessLogger
import scala.sys.process.{ProcessLogger, _}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.io.Source.fromFile
import scala.util.matching.Regex 
object CopyToLocal {

  def main(args: Array[String]): Unit = {
 var kamanja_nodes=ListBuffer[String]()
kamanja_nodes=getStringFromFile("/mnt/beegfs/tools/moveFailed/Nodes.conf").split(",").to[ListBuffer]
    copyLogsToLocal(kamanja_nodes)
}	
	def copyLogsToLocal(nodes:ListBuffer[String])={
for(node<-nodes) {
 logMsg("Copying log file from: "+node)
  var (exitValue, stdoutStream, stderrStream) = runCommand(Seq("scp", node.trim+":/mnt/log_in_ram/FlareLoadCluster/logs/logs/KamanjaManager/*", "/mnt/beegfs/tools/moveFailed/KamanjaLogs/"))
//println(stderrStream)
//println(stdoutStream)
}
}
  def getStringFromFile(path: String): String = {
    val file = getFile(path)
    if (!file.exists())
      throw new FileNotFoundException(file.getAbsolutePath)

    fromFile(file).mkString

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
  private def getFile(path: String): File = {
    if (path == null || path.length == 0) {
      throw new IllegalArgumentException(s"Path is not valid ($path)")
    }
    new File(path)
  }
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def logMsg(msg: String): Unit = {
    val time = LocalDateTime.now().format(formatter)
    println(time + " - " + msg)
  }
}
