#!/bin/sh
exec scala -cp mysql-connector-java-8.0.14.jar -savecompiled "$0" "$@"
!#


import java.io._
import java.nio.file.{Files, StandardCopyOption}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.io.Source.fromFile

object JDBCExtract {

  var configFile = ""
  var sqlFile = ""
  var outputFile = ""
  var delimiter = ""
  var kvTemplate = ""

  def main(args: Array[String]): Unit = {
    parsArgs(args)

    val appConfig = ConfigParser.getAppConfig(configFile)
    val queryTemplate = FileHelper.getStringFromFile(sqlFile)

    val query = getQuery(queryTemplate, kvTemplate)
    val jDBCManager: JDBCManager = new JDBCManager(appConfig.connectionConfig.connectionString)
    try {
      Log.logMsg(s"starting executing query from File:( $sqlFile ) output file: ( $outputFile ) delimiter: ( $delimiter )")
      val queryRunner = new DataExtractor(jDBCManager)
      queryRunner.extractToFile(query, outputFile, delimiter)
      FileHelper.renameDotFile(outputFile)
    } catch {
      case ex: Throwable => {
        Log.logError(s"job failed:\nquery: ${query}\nfile${outputFile}", false, ex)
      }
    } finally {
      if (jDBCManager != null)
        jDBCManager.close()
    }
    Log.logMsg("Exit JDBCExtract")
  }

  def getQuery(templateQuery: String, kv: String): String = {
    val map = kv.split(";").map(s => s.split("=")).map { case Array(k, v) => k -> v }.toMap
    var query = templateQuery
    map.foreach(kv => {
      println(s"${kv._1} -> ${kv._2}")
      query = query.replace(kv._1, kv._2)
    })
    query
  }

  def parsArgs = (args: Array[String]) => {

    def printHelpAndExit(): Unit = {
      Log.logMsg("--configFile --sqlFile --outputFile --delimiter");
      System.exit(0)
    }

    def validateParam(param: String): String = {
      if (param == null || param.length == 0) {
        Log.logMsg(s"param ( $param ) is not valid")
        printHelpAndExit
      }
      param
    }

    args.sliding(2, 2).toList.collect {
      case Array("--configFile", argConfigFile: String) => configFile = validateParam(argConfigFile)
      case Array("--sqlFile", argSqlFile: String) => sqlFile = validateParam(argSqlFile)
      case Array("--outputFile", argOutputFile: String) => outputFile = validateParam(argOutputFile)
      case Array("--delimiter", argDelimiter: String) => delimiter = validateParam(argDelimiter)
      case Array("--kvTemplate", argKvTemplate: String) => kvTemplate = validateParam(argKvTemplate)
      case Array("--help") | _ => println(args.mkString(" ")); printHelpAndExit
    }
  }
}


class DataExtractor(private val jdbcManager: JDBCManager) {
  def extractToFile(query: String, outputFile: String, delimiter: String): Unit = {
    var fileWriter: Writer = null
    Log.logMsg("Executing query: %s".format(query))
    val startTime = System.currentTimeMillis
    val resultSetStatement = jdbcManager.executeQuery(query)
    val resultSet = resultSetStatement._1
    val statement = resultSetStatement._2
    try {
      if (resultSetStatement != null) {
        fileWriter = getFileWriter(outputFile)
        val metaData = resultSet.getMetaData
        val columnsName: ArrayBuffer[String] = ArrayBuffer()
        for (c <- 1 to metaData.getColumnCount) {
          val cName = metaData.getColumnName(c)
          columnsName += cName
        }
        val columnNamesList: Seq[String] = columnsName.toList
        Log.logMsg(s"columns to be extracted:\n${columnNamesList.mkString(delimiter)}")
        while (resultSet.next()) {
          val row = ArrayBuffer[String]()
          columnNamesList.foreach(columnName => {
            val columnValue = resultSet.getString(columnName)
            row += columnValue
          })
          fileWriter.write(row.mkString(delimiter))
        }
        val endTime = System.currentTimeMillis
        val milliSec = endTime - startTime
        val sec = milliSec / 1000
        Log.logMsg(s"summary: file: ${outputFile},lines: ${fileWriter.getCurrentRecordCount}, stime:$startTime etime:${endTime},sec:${sec},millis:${milliSec}")
      }
    } catch {
      case ex: Throwable => {
        Log.logError("", false, ex)
      }
    } finally {
      try {
        if (resultSet != null)
          resultSet.close()
        if (statement != null)
          statement.close()
        if (fileWriter != null)
          fileWriter.close()
      } catch {
        case ex: Throwable => {
          Log.logError("", false, ex)
        }
      }
    }
  }

  def getFileWriter(fileName: String): Writer = {
    val bufferedWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fileName))))
    new GenericFileWriter(bufferedWriter, fileName)
  }
}

trait Writer {
  def getCurrentRecordCount(): Long;

  def write(line: String): Unit;

  def close(): Unit;
}

class GenericFileWriter(private val bufferWriter: BufferedWriter, private val fileName: String) extends Writer {
  private var lineCounts = 0;

  override def getCurrentRecordCount(): Long = lineCounts

  override def write(line: String): Unit = {
    bufferWriter.write(line)
    bufferWriter.newLine()
    lineCounts += 1
    if (lineCounts % 10000 == 0)
      Log.logMsg(s"${lineCounts} lines have been written to file: $fileName so far")
  }

  override def close(): Unit = {
    try {
      if (bufferWriter != null)
        bufferWriter.close()

    } catch {
      case ex: Throwable => {
        Log.logError("", false, ex)
      }
    }
  }
}

class JDBCManager(private val url: String) {

  Log.logMsg("connecting...")
  private val conn: Connection = DriverManager.getConnection(this.url)
  Log.logMsg("connected")

  private val maxRetry = 3


  def executeQuery(query: String): (ResultSet, Statement) = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    while (resultSet == null && retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          statement = conn.createStatement()
          resultSet = statement.executeQuery(query)
        }
      } catch {
        case ex: Throwable => {
          retry = retry + 1
          Log.logError("Exception while executing query: %s ".format(query), false, ex)
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
        }
      }
    }
    (resultSet, statement)
  }

  def close() {
    if (conn != null && !conn.isClosed)
      conn.close()
  }
}

object FileHelper {
  val FILE_SEPARATOR: String = System.getProperty("file.separator")

  def getFile(path: String): File = {
    if (path == null || path.length == 0) {
      throw new IllegalArgumentException(s"Path is not valid ($path)")
    }
    new File(path)
  }

  def moveFile(p1: String, p2: String): Unit = {
    val f1 = getFile(p1)
    val f2 = getFile(p2)
    moveFile(f1, f2)
  }

  def moveFile(f1: File, f2: File): Unit = {
    Files.move(f1.toPath, f2.toPath, StandardCopyOption.REPLACE_EXISTING)
  }

  def getStringFromFile(path: String): String = {
    val file = getFile(path)
    getStringFromFile(file)
  }

  def getStringFromFile(file: File): String = {
    if (!file.exists())
      throw new FileNotFoundException(file.getAbsolutePath)
    fromFile(file).mkString
  }

  def getLinesFromFile(path: String): List[String] = {
    val file = getFile(path)
    getLinesFromFile(file)
  }

  def getLinesFromFile(file: File): List[String] = {
    fromFile(file).getLines().toList
  }

  def writeNewFile(fileName: String, content: List[String], append: Boolean): Unit = {
    val file = getFile(fileName)
    writeNewFile(file, content, append)
  }

  def writeNewFile(file: File, content: List[String], append: Boolean): Unit = {
    try {
      val bw = new BufferedWriter(new FileWriter(file, append))
      content.foreach(f => {
        bw.write(f)
        //bw.newLine
      }
      )
      bw.close()
    }
    catch {
      case e: Exception => {
        throw new Exception("Exception while writing into file ${fileName}", e)
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

  def renameDotFile(fullFileName: String): Unit = {
    if (fullFileName != null && fullFileName.length >= 0) {
      val file = FileHelper.getFile(fullFileName)
      val fileName = file.getName
      if (fileName.startsWith(".") && fileName.size > 1) {
        val dirPath = file.getParentFile.getAbsolutePath
        val newFullFileName = dirPath + FileHelper.FILE_SEPARATOR + fileName.substring(1)
        Log.logMsg(s"renaming ( ${fullFileName} ) to ( ${newFullFileName} )")
        FileHelper.moveFile(fullFileName, newFullFileName)
      }
    }
  }

}

object Log {
  private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def logMsg(msg: String): Unit = {
    val time = LocalDateTime.now().format(formatter)
    println(time + " - " + msg)
  }

  def logError(msg: String, exit: Boolean, throwable: Throwable = null): Unit = {
    val time = LocalDateTime.now().format(formatter)
    System.err.println(time + " - %s".format(msg))
    if (throwable != null) {
      System.err.println(getExceptionString(throwable))
    }
    if (exit) {
      System.exit(-1)
    }
  }

  private def getExceptionString(throwable: Throwable): String = {
    val sw = new StringWriter
    throwable.printStackTrace(new PrintWriter(sw))
    val exceptionAsString = sw.toString
    sw.close
    exceptionAsString
  }
}

object ConfigParser {
  private var appConfig: AppConfig = _

  def getAppConfig(configFile: String): AppConfig = {
    if (appConfig != null) appConfig else {
      val properties = new Properties()
      val in = new FileInputStream(configFile)
      properties.load(in)
      in.close()

      val connectionConfig = getConnectionConfig(properties)
      appConfig = AppConfig(connectionConfig)
      appConfig
    }
  }
}


private def getConnectionConfig(p: Properties): ConnectionConfig = {
  val connectionString = p.getProperty("connection.string", "")
  ConnectionConfig(connectionString)
}

case class AppConfig(connectionConfig: ConnectionConfig)

case class ConnectionConfig(connectionString: String)


JDBCExtract.main(args)
