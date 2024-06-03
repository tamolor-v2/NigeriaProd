#!/bin/sh
exec scala -cp /mnt/beegfs/tools/Consolidation/config:/mnt/beegfs/tools/Consolidation/lib/Consolidation_2.11.11-1.0.0.jar:/mnt/beegfs/tools/Consolidation/lib/DependencyLibs_2.11.11-1.0.jar:/mnt/beegfs/tools/Consolidation/lib/jcommander-1.64.jar:/mnt/beegfs/tools/Consolidation/lib/presto-jdbc-0.191.jar:/usr/hdp/current/hadoop-client/conf -savecompiled "$0" "$@"
!#


import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.time.ZonedDateTime
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.beust.jcommander.{JCommander, ParameterException}
import com.ligadata.consolidation.fileUtils.{FileHandler, FileHandlerFactory}
import com.ligadata.consolidation.utils.JsonHelper
import org.apache.logging.log4j.{LogManager, Logger}

import scala.collection.mutable.{ArrayBuffer => MuArray}
import scala.io.Source


case class FeedInfo(orgLocation: String, subFolder: String, harDir: String, harFileName: String)

class Summary {
  var feedName: String = _ // done
  var dataDate: String = _ // done
  var startTime: String = _ // done
  var endTime: String = _ //done
  var elapsed: String = _ //done
  var nFilesBefore: Int = -1 // done
  var nFilesAfter: Int = -1 // done
  var qResultBefore: String = _
  var qResultAfter: String = _
  var status: String = _ //done

  override def toString: String = {
    s"""{"feedName":"$feedName","dataDate":"$dataDate","startTime":"$startTime","endTime":"$endTime","elapsed":"$elapsed","nFilesBefore":"$nFilesBefore","nFilesAfter":"$nFilesAfter","qResultBefore":"$qResultBefore","qResultAfter":"$qResultAfter","status":"$status"}"""
  }
}

object Driver {

  private val logger: Logger = LogManager.getLogger(this.getClass)
  private val loggerSummary: Logger = LogManager.getLogger("consolidation.summary")
  private var fileHandler: FileHandler = _

  def getFileHandler(configFilePath: String): FileHandler = {
    val configJson = Source.fromFile(configFilePath).mkString
    val conf = JsonHelper.deserializeConnectionConfig(configJson)
    val fileHandler: FileHandler = FileHandlerFactory.createSmartFileHandler(conf, "/")
    fileHandler
  }

  def main(args: Array[String]): Unit = {
    parseCmd(args)
    Config.parseConfig(CmdLine.getConfigFile)
    val filteredFeeds = CmdLine.getFeeds.filter(f => !CmdLine.getSkipFeeds.contains(f)).map(f => Config.getFeedCfg(f))
    logger.info(s"filteredFeeds:${filteredFeeds.map(_.name)}")
    val stTime = ZonedDateTime.now
    val jDBCManager = new JDBCManager(Config.getPrestoCfg.connectionString, Config.getPrestoCfg.user, Config.getPrestoCfg.password)
    val vHandler: ValidationHandler = new ValidationHandler(jDBCManager)

    fileHandler = getFileHandler(Config.getConsolidationCfgTemplate)

    val days = getDays
    logger.info("days===>" + days)
    days.foreach(day => {
      logger.info(s"working on day :$day")
      filteredFeeds.foreach(f => {
        val summary = new Summary
        try {
          logger.info(s"working on feed:${f.name}")
          val feedSTime = ZonedDateTime.now
          summary.feedName = f.name
          summary.dataDate = day

          val feedWorkDir = createFeedWorkDir(f)
          val validationResultBefore = runVQuery(f.vQueries, vHandler, day)

          summary.qResultBefore = validationResultBefore.map(m => m.map(a => a.mkString("|")).mkString("~")).mkString(";")

          val feedInfo = createHarFile(f, day)
          val fullPathToBeConsolidated = feedInfo.orgLocation + FileHelper.FILE_SEPARATOR + feedInfo.subFolder

          summary.nFilesBefore = fileHandler.listFiles(fullPathToBeConsolidated, 1, false).length
          logger.info(s"files in dir: $fullPathToBeConsolidated before: " + summary.nFilesBefore)

          val consolidationCfg = prepareConsolidationCfgFile(f, feedWorkDir, day)
          logger.info(s"start consolidation process for feed:${f.name}, date:$day")
          com.ligadata.consolidation.Main.main(Array(consolidationCfg))
          logger.info(s"end consolidation process for feed:${f.name}, date:$day")

          summary.nFilesAfter = fileHandler.listFiles(fullPathToBeConsolidated, 1, false).length
          logger.info(s"files in dir: $fullPathToBeConsolidated files after: " + summary.nFilesAfter)

          logger.info("running validation query after")
          val validationResultAfter = runVQuery(f.vQueries, vHandler, day)

          summary.qResultAfter = validationResultAfter.map(m => m.map(a => a.mkString("|")).mkString("~")).mkString(";")


          val matched = compareQueryResult(validationResultBefore, validationResultAfter)
          var timeInfo: TimeInfo = null
          if (matched) {
            timeInfo = DateTimeUtil.getTimeInfo(feedSTime)
            deleteHarFile(feedInfo)
            summary.status = "succeeded"
            logger.info(s"feed:${f.name} date:$day consolidated successfully in ${timeInfo.elapsed}")
          } else {
            deleteConsolidationFolder(feedInfo)
            restoreHarFile(feedInfo)

            val validationResultAfterRestore = runVQuery(f.vQueries, vHandler, day)
            val matchedAfterRestore = compareQueryResult(validationResultBefore, validationResultAfterRestore)
            summary.qResultAfter = validationResultAfterRestore.map(m => m.map(a => a.mkString("|")).mkString("~")).mkString(";")
            if (matchedAfterRestore) {
              deleteHarFile(feedInfo)
              timeInfo = DateTimeUtil.getTimeInfo(feedSTime)
              logger.info(s"feed:${f.name} date:$day failed to consolidate. Data was restored successfully from har file successfully in ${timeInfo.elapsed}")
              summary.status = "restored"
            } else {
              timeInfo = DateTimeUtil.getTimeInfo(feedSTime)
              logger.info(s"feed:${f.name} date:$day failed to consolidate. validation numbers still don't match even after restore. har file location:${feedInfo.harDir} ${feedInfo.harFileName} ")
              summary.status = "failed"

            }
          }
          summary.startTime = timeInfo.startTime
          summary.endTime = timeInfo.endTime
          summary.elapsed = timeInfo.elapsed
          loggerSummary.info(summary.toString)

        } catch {
          case ex: Throwable =>
            logger.error(s"error while working on feed:${f.name} day:$day", ex)
            loggerSummary.info(summary.toString)

        }
      })
    })
    val finalTimeInfo = DateTimeUtil.getTimeInfo(stTime)
    logger.info(s"Done all feeds in ${finalTimeInfo.elapsed}")
  }

  def getDays: List[String] = { // todo handle days from several months
    if (CmdLine.getStartDate.isEmpty)
      List()
    val sDate = CmdLine.getStartDate.replace("-", "").toInt
    val eDate = CmdLine.getEndDate.replace("-", "").toInt
    DateTimeUtil.getDateRange(sDate, eDate).map(d => DateTimeUtil.parseDataDate(d.toString).format(DateTimeUtil.`yyyy-MM-dd`))
  }

  def getInputPath(feedCfg: Feed, day: String): String = {
    val stDate = DateTimeUtil.parseDataDate(day, DateTimeUtil.`yyyy-MM-dd`).format(DateTimeUtil.yyyyMMdd)
    val srcDir = if (feedCfg.srcDir != null && feedCfg.srcDir.nonEmpty) feedCfg.srcDir else Config.getGlobalLocationsCfg.srcDir
    val srcFullDir = srcDir + FileHelper.FILE_SEPARATOR + feedCfg.name + "tbl_dt" + stDate
    srcFullDir
  }

  def deleteConsolidationFolder(harInfo: FeedInfo): Unit = {
    val fullConsolidationFolder = harInfo.orgLocation + FileHelper.FILE_SEPARATOR + harInfo.subFolder
    logger.info(s"deleting ConsolidationFolder: ${fullConsolidationFolder}")

    //logger.info(s"executing command: hadoop fs -rm -r $fullConsolidationFolder")
    val command: String = s"hadoop fs -rm -r $fullConsolidationFolder"
    SysCommand.runSysCmd(command.split(" ").toSeq, errorSb = null)
  }

  def restoreHarFile(harInfo: FeedInfo): Unit = {
    val fullHarLocation = harInfo.harDir + FileHelper.FILE_SEPARATOR + harInfo.harFileName + "/*"
    val fullOrgLocation = harInfo.orgLocation + FileHelper.FILE_SEPARATOR
    logger.info(s"restoring from har file :$fullHarLocation to original location $fullOrgLocation")

    //logger.info(s"executing command: hdfs dfs -cp har://$fullHarLocation hdfs:$fullOrgLocation")

    val command: String = s"hdfs dfs -cp har://$fullHarLocation hdfs:$fullOrgLocation"
    SysCommand.runSysCmd(command.split(" ").toSeq, errorSb = null)
  }

  def deleteHarFile(harInfo: FeedInfo): Unit = {
    val fullOutputHarFile = harInfo.harDir + FileHelper.FILE_SEPARATOR + harInfo.harFileName
    logger.info(s"deleting har file : ${fullOutputHarFile}")

    //logger.info(s"executing command: hadoop fs -rm -r $fullOutputHarFile")

    val command: String = s"hadoop fs -rm -r $fullOutputHarFile"
    SysCommand.runSysCmd(command.split(" ").toSeq, errorSb = null)
  }

  def compareQueryResult(before: Array[Array[Array[String]]], after: Array[Array[Array[String]]]): Boolean = {
    if (before.length == after.length) { // making sure number of queries are matched

      for (i <- 0 until before.length) {
        val bRows = before(i) // number of rows
        val aRows = after(i) // number of rows

        if (bRows.length == aRows.length) {
          for (j <- 0 until bRows.length) {
            val bCols = bRows(j)
            val aCols = aRows(j)
            logger.debug(s"row #$j bCols:${bCols.mkString(";")}")
            logger.debug(s"row #$j aCols:${bCols.mkString(";")}")
            if (bCols.length == aCols.length) {
              for (c <- 0 until bCols.length) {
                val bValue = bCols(c)
                val aValue = aCols(c)
                //logger.debug(s"bValue:${bValue}, aValue:${aValue}")
                if (bValue != aValue || (bValue.isEmpty && aValue.isEmpty) || (bValue == "0" && aValue == "0")) {
                  logger.warn(s"query #$i, row #$j field #$c bValue: $bValue aValue: $aValue")
                  return false
                }
              }
            } else {
              logger.warn(s"query #$i, row #$j don't have same number of columns  bValue: ${bCols.length} aValue: ${aCols.length}")
              return false
            }
          }
        } else {
          logger.warn(s"query #$i, doesn't have same number of records   bValue: ${bRows.length} aValue: ${aRows.length}")
          return false
        }
      }
    } else {
      logger.warn(s"number of queries don't match ")
      return false
    }
    true
  }

  def createHarFile(feedCfg: Feed, day: String): FeedInfo = { // todo currently only one day is supported && delete har file if exists
    logger.info(s"creating har file for feed:${feedCfg.name} if not exits")
    val errorStringBuilder = new StringBuilder
    val commandTemplate = "hadoop||archive||-archiveName||${harFileName}||-p||${PARENT_FOLDER}||${SUB_FOLDERS}||${OUTPUT_FOLDER}"
    val stDate = DateTimeUtil.parseDataDate(day, DateTimeUtil.`yyyy-MM-dd`).format(DateTimeUtil.yyyyMMdd)
    //val endDate = DateTimeUtil.parseDataDate(CmdLine.getEndDate, DateTimeUtil.`yyyy-MM-dd`).format(DateTimeUtil.yyyyMMdd)
    val srcDir = if (feedCfg.srcDir != null && feedCfg.srcDir.nonEmpty) feedCfg.srcDir else Config.getGlobalLocationsCfg.srcDir
    val harDir = if (feedCfg.harDir != null && feedCfg.srcDir.nonEmpty) feedCfg.harDir else Config.getGlobalLocationsCfg.harDir

    val harName = feedCfg.name + "_" + stDate + ".har"
    val srcLocation = srcDir + FileHelper.FILE_SEPARATOR + feedCfg.name
    val subFolders = "tbl_dt=" + stDate
    val destinationLocation = harDir + FileHelper.FILE_SEPARATOR + feedCfg.name
    val harInfo = FeedInfo(srcLocation, subFolders, destinationLocation, harName)
    logger.info(s"harInfo: orgLocation${harInfo.orgLocation},subFolder:${harInfo.subFolder},harDir:${harInfo.harDir},harFileName:${harInfo.harFileName}")
    val fullHarFileDir = destinationLocation + FileHelper.FILE_SEPARATOR + harName

    val exists = isHDFSPath(fullHarFileDir)

    if (!exists) {
      logger.info("creating new har file")
      val templatePlaceHolders: Map[String, String] = Map(
        "${harFileName}" -> harInfo.harFileName,
        "${PARENT_FOLDER}" -> harInfo.orgLocation,
        "${SUB_FOLDERS}" -> harInfo.subFolder,
        "${OUTPUT_FOLDER}" -> harInfo.harDir
      )
      var cmd = commandTemplate
      templatePlaceHolders.foreach(kv => {
        cmd = cmd.replace(kv._1, kv._2)
      })
      val returnCode = SysCommand.runSysCmd(cmd.split("\\|\\|"), errorSb = errorStringBuilder)
      if (returnCode != 0) {
        throw new Exception(errorStringBuilder.toString)
      }
    } else {
      logger.info(s"using existing har file:$fullHarFileDir")
    }

    // logger.info(s"executing command: $cmd")


    harInfo
  }

  def isHDFSPath(path: String): Boolean = {
    logger.info(s"checking if $path exists")
    val errorStringBuilder = new StringBuilder
    val commandTemplate = "hadoop||fs||-test||-e||${path}"
    val cmd = commandTemplate.replace("${path}", path)
    val returnCode = SysCommand.runSysCmd(cmd.split("\\|\\|"), errorSb = errorStringBuilder)
    logger.info(s"is hdfs path:$path => ${returnCode}")
    returnCode == 0
  }

  def runVQuery(vQuerysTemplates: List[String], vHandler: ValidationHandler, day: String): Array[Array[Array[String]]] = {
    logger.info("running validation query before")
    try {
      val templatePlaceHolders: Map[String, String] = Map(
        "${START_DATE}" -> day.replace("-", ""),
        "${END_DATE}" -> day.replace("-", "") // todo at this moment we care about only start_date
      )
      val allResult: MuArray[Array[Array[String]]] = MuArray()
      vQuerysTemplates.foreach(vq => {
        var query: String = vq
        templatePlaceHolders.foreach(kv => {
          query = query.replace(kv._1, kv._2)
        })
        allResult += vHandler.runQuery(query)
      })
      allResult.toArray
    } catch {
      case ex: Throwable =>
        logger.error("Exception while running query ", ex)
        throw ex
    }
  }

  def prepareConsolidationCfgFile(feedCfg: Feed, workDir: String, day: String): String = {
    logger.info(s"creating consolidation cfg file for feed:${feedCfg.name}")
    val template = FileHelper.getStringFromFile(Config.getConsolidationCfgTemplate)
    val srcDir = if (feedCfg.srcDir != null && feedCfg.srcDir.nonEmpty) feedCfg.srcDir else Config.getGlobalLocationsCfg.srcDir

    val templatePlaceHolders: Map[String, String] = Map(
      "${SRC_DIR}" -> (srcDir + FileHelper.FILE_SEPARATOR + feedCfg.name),
      "${START_DATE}" -> day,
      "${END_DATE}" -> day //CmdLine.getEndDate
    )
    var newCfg: String = template
    templatePlaceHolders.foreach(kv => {
      newCfg = newCfg.replace(kv._1, kv._2)
    })
    val newCfgFileName = workDir + FileHelper.FILE_SEPARATOR + feedCfg.name + "_" + System.currentTimeMillis() + ".cfg"
    logger.info(s"writing cfg file: ${newCfgFileName} ")
    FileHelper.writeNewFile(newCfgFileName, List(newCfg), append = false)
    newCfgFileName
  }

  def createFeedWorkDir(feedCfg: Feed): String = {
    val workDirName = CmdLine.getWorkDir + FileHelper.FILE_SEPARATOR + feedCfg.name + "_" + System.currentTimeMillis()
    FileHelper.mkDir(workDirName)
    logger.info(s"working dir: $workDirName")
    workDirName
  }


  /**
    * function to parse command line array
    *
    * @param arr
    */
  private def parseCmd(arr: Array[String]): Unit = {

    val jCommander: JCommander = new JCommander()
    jCommander.addObject(CmdLine)
    try {
      //jCommander.parse(args.toArray: _*)
      jCommander.parse(arr.toArray: _*)

      if (CmdLine.isHelp) {
        jCommander.usage()
        System.exit(-1)
      }
    }
    catch {
      case e: ParameterException => {
        jCommander.usage()
        logger.error("Invalid arguments.", e)
        System.exit(-1)
      }
    }
  }
}


object SysCommand {
  private val logger: Logger = LogManager.getLogger(this.getClass)

  def runSysCmd(argsList: Seq[String], outputSb: StringBuilder = null, errorSb: StringBuilder = null, timeOut: Long = 60, maxRetry: Int = 1): Int = {
    var procRetCode = -1000
    val mRetry = if (maxRetry > 0) maxRetry else 1
    var retry = 0
    var isFinished = false
    var pr: Process = null
    try {
      val pool: ExecutorService = Executors.newFixedThreadPool(2)
      val bufferReaders = MuArray[BufferedReader]()
      while (retry <= mRetry && !isFinished) {
        logger.info("executing command: %s".format(argsList.mkString(" ")))
        try {
          val procBuilder = new java.lang.ProcessBuilder(argsList: _*)
          pr = procBuilder.start()
          addInputStreamThreadPool(pool, pr, bufferReaders, pr.getErrorStream, errorSb)
          addInputStreamThreadPool(pool, pr, bufferReaders, pr.getInputStream, outputSb)
          isFinished = pr.waitFor(timeOut, TimeUnit.MINUTES)
          procRetCode = pr.exitValue()
        }
        catch {
          case e: Throwable =>
            try {
              retry = retry + 1
              val msg = "Timeout when executed command: %s .Timeout is %2d minutes. retry:%2d/%2d".format(argsList.mkString(" "), timeOut, retry, maxRetry)
              logger.error(msg, e)
              errorSb.append(e.getMessage)
              pr.destroyForcibly()
            } catch {
              case ex: Throwable =>
                logger.error("", ex)
            }
        }
      }
      pool.shutdownNow()
      pool.awaitTermination(1, TimeUnit.HOURS)
      bufferReaders.foreach(br => {
        try {
          if (br != null)
            br.close()
        } catch {
          case e: Throwable =>
            logger.error("", e)
        }
      })
    } catch {
      case e: Throwable =>
        logger.error("", e)
    }
    procRetCode
  }


  private def addInputStreamThreadPool(pool: java.util.concurrent.ExecutorService, proc: Process, bufferReads: MuArray[BufferedReader], is: InputStream, sb: StringBuilder = null): Unit = {
    try {
      pool.execute(new Runnable() {
        val inputStream: InputStream = is

        override def run(): Unit = {
          var bufferedReader: BufferedReader = null
          try {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
            bufferReads += bufferedReader
            var line: String = null
            line = bufferedReader.readLine
            while (!pool.isShutdown && !pool.isTerminated && proc.isAlive && line != null) {
              println(line)
              if (sb != null)
                sb.append(line).append("\n")
              line = bufferedReader.readLine
            }
          }
          catch {
            case e: Throwable =>
              e.printStackTrace()
          } finally {
            if (bufferedReader != null)
              bufferedReader.close()
          }
        }
      })
    } catch {
      case e: Throwable =>
        e.printStackTrace()
    }
  }

  import sys.process._

  def runSysCmd(cmd: Seq[String]): (Int, String, String) = {
    val c = cmd.mkString(" ")
    logger.info("executing command: %s".format(c))
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val codeResult = cmd ! ProcessLogger(stdout append "\n" append _, stderr append _);
    (codeResult, stdout.toString, stderr.toString)
  }
}


import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.logging.log4j.{LogManager, Logger}

import scala.collection.mutable.{ArrayBuffer => MuArray, Map => MuMap}


class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val logger: Logger = LogManager.getLogger(this.getClass)

  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

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
          logger.error("Exception while executing query: %s ".format(query), ex)
          logger.warn("Retry: %2d/%2d".format(retry, maxRetry))
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


class ValidationHandler(private val jdbcManager: JDBCManager) {
  private val logger: Logger = LogManager.getLogger(this.getClass)


  def runQuery(query: String): Array[Array[String]] = {
    val rs: MuArray[Array[String]] = MuArray[Array[String]]()
    logger.info("Executing query: %s".format(query))
    val resultSetStatement = jdbcManager.executeQuery(query)
    val resultSet = resultSetStatement._1
    val statement = resultSetStatement._2
    try {
      if (resultSetStatement != null) {
        val metaData = resultSet.getMetaData
        val columnsName: MuArray[String] = MuArray()
        for (c <- 1 to metaData.getColumnCount) {
          columnsName += metaData.getColumnName(c)
        }
        val columnNamesList = columnsName
        rs += columnNamesList.toArray
        while (resultSet.next()) {
          val row = MuArray[String]()
          columnNamesList.foreach(columnName => {
            val columnValue = resultSet.getString(columnName)
            row += columnValue
          })
          logger.debug(s"row=>> ${row.mkString(",")}")
          rs += row.toArray
        }
      }
    } catch {
      case ex: Throwable =>
        logger.error("", ex)
    } finally {
      try {
        if (resultSet != null)
          resultSet.close()
        if (statement != null)
          statement.close()
      } catch {
        case ex: Throwable =>
          logger.error("", ex)
      }
    }
    rs.toArray
  }
}


import java.io.{BufferedWriter, File, FileNotFoundException, FileWriter}
import java.nio.file.{Files, StandardCopyOption}
import java.time._
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer
import scala.io.Source.fromFile

object DateTimeUtil {
  val yyyyMMddHHmmss: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val `yyyy-MM-dd HH:mm:ssZ`: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ")
  val `yyyy-MM-dd`: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val `HH:mm:ss.SSS`: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
  val yyyyMMdd: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val dd: DateTimeFormatter = DateTimeFormatter.ofPattern("dd")

  def getTimeInfo(startTime: ZonedDateTime): TimeInfo = {
    val endTime = ZonedDateTime.now
    val duration = Duration.between(startTime, endTime)
    val elapsed = LocalDateTime.MIN.plusSeconds(duration.getSeconds).format(`HH:mm:ss.SSS`)
    TimeInfo(startTime.format(`yyyy-MM-dd HH:mm:ssZ`), endTime.format(`yyyy-MM-dd HH:mm:ssZ`), elapsed)
  }

  def getCurrentDate(format: DateTimeFormatter): String = LocalDateTime.now().format(format)

  def parseDataDate(dataDate: String): LocalDate = LocalDate.parse(dataDate, yyyyMMdd)

  def parseDataDate(dataDate: String, format: DateTimeFormatter): LocalDate = LocalDate.parse(dataDate, format)

  def getMDay(date: LocalDate): String = date.format(DateTimeUtil.dd)

  def getDaysBefore(date: LocalDate, days: Int, formatter: DateTimeFormatter): String = date.minusDays(days).format(formatter)

  def getMonthStartDay(date: LocalDate): String = date.withDayOfMonth(1).format(DateTimeUtil.yyyyMMdd)

  def getDateRange(startDate: Int, endDate: Int): List[Int] = {
    if (startDate == endDate)
      return List(startDate)


    val sd = parseDataDate(startDate.toString)
    val ed = parseDataDate(endDate.toString)

    val days = Period.between(sd, ed).getDays
    val dateRange = ArrayBuffer[String]()
    if (days > 0) {
      var curr = sd
      while (Period.between(curr, ed).getDays != 0) {
        dateRange += curr.format(yyyyMMdd)
        curr = curr.plusDays(1)
      }
      dateRange += curr.format(yyyyMMdd)
    }
    dateRange.map(_.toInt).toList
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

  def exits(file: String): Boolean = getFile(file).exists()

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

  def writeNewFile(fileName: String, content: List[String], append: Boolean): Unit = {
    val file = getFile(fileName)
    try {
      val bw = new BufferedWriter(new FileWriter(file, append))
      content.foreach(f => {
        bw.write(f)
        bw.newLine
      }
      )
      bw.close()
    }
    catch {
      case e: Exception =>
        throw new Exception(s"Exception while writing into file $fileName", e)
      //Log.logError(s"Exception while writing into file ${fileName}", e, true)
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


case class TimeInfo(startTime: String, endTime: String, elapsed: String)


import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.JsonMethods.parse
import org.apache.logging.log4j.{LogManager, Logger}
import org.json4s.jackson.Serialization
import scala.collection.mutable.{Map => MuMap}
import scala.io.Source.fromFile

object Config {
  private implicit val jsonFormats: Formats = DefaultFormats
  private val logger: Logger = LogManager.getLogger(this.getClass)

  private val feeds: MuMap[String, Feed] = MuMap()
  private val globalLocations: GlobalCfg = new GlobalCfg
  private val prestoCfg: PrestoCfg = new PrestoCfg
  private var consolidationCfgTemplate: String = _

  def getFeedsCfg: Map[String, Feed] = feeds.toMap

  def getFeedCfg(feedName: String): Feed = feeds.getOrElse(feedName, null)

  def getGlobalLocationsCfg: GlobalCfg = this.globalLocations

  def getPrestoCfg: PrestoCfg = this.prestoCfg

  def getConsolidationCfgTemplate: String = this.consolidationCfgTemplate

  /**
    * method to be called to parse jsonStr string and populate _feedsConfigByName & _feedsLocationsConfigByName maps
    *
    * @param filePath string
    */
  def parseConfig(filePath: String): Unit = {
    logger.info(s"reading config file: $filePath ")
    try {
      val jsonStr = fromFile(filePath).getLines.mkString("\n")
      logger.info(s"parsing config: ${jsonStr}")
      val jValue = parse(jsonStr)
      val cfg = jValue.extract[Map[String, Any]]
      setGlobalCfg(cfg)
      setPrestoCfg(cfg)
      setFeeds(cfg)
      setConsolidationCfgTemplate(cfg)
    } catch {
      case ex: Throwable => {
        logger.error(s"failed to parse config jsonStr", ex)
        throw new Exception(ex)
      }
    }
  }

  private def setConsolidationCfgTemplate(cfg: Map[String, Any]): Unit = {
    val consolidationCfg = cfg.getOrElse("consolidation.tool.config.template", "/tmp/template.cfg").asInstanceOf[String]
    this.consolidationCfgTemplate = consolidationCfg
  }

  private def setGlobalCfg(cfg: Map[String, Any]): Unit = {
    val globalCfgMap = cfg.getOrElse("global", Map[String, Any]()).asInstanceOf[Map[String, Any]]
    if (globalCfgMap.contains("src.dir")) {
      this.globalLocations.srcDir = globalCfgMap("src.dir").asInstanceOf[String]
    }
    if (globalCfgMap.contains("har.dir")) {
      this.globalLocations.srcDir = globalCfgMap("har.dir").asInstanceOf[String]
    }
  }

  private def setPrestoCfg(cfg: Map[String, Any]): Unit = {
    val prestoCfgMap = cfg.getOrElse("presto.config", Map[String, Any]()).asInstanceOf[Map[String, Any]]
    if (prestoCfgMap.contains("connection.string")) {
      this.prestoCfg.connectionString = prestoCfgMap("connection.string").asInstanceOf[String]
    }
    if (prestoCfgMap.contains("username")) {
      this.prestoCfg.user = prestoCfgMap("username").asInstanceOf[String]
    }
    if (prestoCfgMap.contains("password")) {
      this.prestoCfg.password = prestoCfgMap("password").asInstanceOf[String]
    }
  }

  private def setFeeds(cfg: Map[String, Any]): Unit = {
    val feedsMap = cfg.getOrElse("feeds", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]

    feedsMap.foreach(feed => {
      val f = new Feed
      if (feed.contains("name")) {
        f.name = feed("name").asInstanceOf[String]

        if (f.name.isEmpty || f.name == "*")
          throw new IllegalArgumentException("feed name cannot be empty or *")
        logger.debug(s"name:${f.name}")
      }
      if (feed.contains("v.queries")) {
        f.vQueries = feed("v.queries").asInstanceOf[List[String]]
        logger.debug(s"v.queries:${f.vQueries}")
      }
      if (feed.contains("src.dir")) {
        f.srcDir = feed("src.dir").asInstanceOf[String]
        logger.debug(s"srcDir:${f.srcDir}")
      }
      if (feed.contains("har.dir")) {
        f.harDir = feed("har.dir").asInstanceOf[String]
        logger.debug(s"har.dir:${f.harDir}")

      }
      this.feeds.put(f.name, f)
    })

  }


}

class PrestoCfg {
  var connectionString: String = _
  var user: String = _
  var password: String = _
}


class Feed {
  var name: String = _
  var vQueries: List[String] = _
  var srcDir: String = _
  var harDir: String = _
}

class GlobalCfg {
  var srcDir: String = _
  var harDir: String = _
}


import com.beust.jcommander.Parameter

object CmdLine {

  @Parameter(names = Array("--cfg"), description = "config file ", required = true)
  private var configFile: String = ""

  @Parameter(names = Array("--workDir"), description = "workDir", required = false)
  private var workDir: String = "/tmp"

  @Parameter(names = Array("--feeds"), description = "comma separated feeds name or all", required = true)
  private var feeds: String = "all"

  @Parameter(names = Array("--skipFeeds"), description = "comma separated feeds name", required = false)
  private var skipFeeds: String = ""

  @Parameter(names = Array("--startDate"), description = "start date in yyyy-MM-dd ", required = true)
  private var startDate: String = ""

  @Parameter(names = Array("--endDate"), description = "end date in yyyy-MM-dd ", required = false)
  private var endDate: String = ""

  @Parameter(names = Array("-h", "--help"), description = "Display usage ", help = true)
  private var help: Boolean = false

  def getConfigFile: String = this.configFile

  def getWorkDir: String = this.workDir

  def getFeeds: List[String] = this.feeds.split(",").toList

  def getSkipFeeds: Set[String] = this.skipFeeds.split(",").toSet

  def getStartDate: String = this.startDate

  def getEndDate: String = if (!this.endDate.isEmpty) this.endDate else this.startDate


  def isHelp: Boolean = help

}

Driver.main(args)
