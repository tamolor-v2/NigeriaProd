#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar  -savecompiled "$0" "$@"
!#

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Calendar, TimeZone}

import scala.collection.mutable.{ArrayBuffer => MuArray, Set => MuSet}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DailyValSegCalc {
  object StageNumbers extends Enumeration {
    val stageNONE, stageONE = Value
  }

  val extraSdpPrevDays = 3

  object Log {
    private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def logMsg(msg: String): Unit = {
      val time = LocalDateTime.now().format(formatter)
      println(time + " - " + msg)
    }

    def logError(msg: String, throwable: Throwable = null): Unit = {
      val time = LocalDateTime.now().format(formatter)
      System.err.println(time + " - %s".format(msg))
      if (throwable != null) {
        System.err.println(getExceptionString(throwable))
      }
    }

    private def getExceptionString(throwable: Throwable): String = {
      val sw = new StringWriter
      throwable.printStackTrace(new PrintWriter(sw))
      val exceptionAsString = sw.toString
      sw.close()
      exceptionAsString
    }
  }

  class JDBCManager(private val url: String, private val user: String, private val pass: String, shutdownInfo: ShutdownInfo) {
    private var conn: Connection = null

    private val maxRetry = 13

    def runDeleteOrInsertQuery(cmdPrefixStr: String, query: String): Unit = {
      try {
        Log.logMsg("%s executing command: %s".format(cmdPrefixStr, query))
        val qryResult = executeUpdate(query)
        Log.logMsg("%s executing command returned %d results".format(cmdPrefixStr, qryResult))
      } catch {
        case ex: Throwable => {
          Log.logError("%s Failed", ex)
        }
      }
    }

    private def getConnection(): Connection = {
      if (conn != null && !conn.isClosed)
        return conn

      close

      conn = DriverManager.getConnection(this.url, this.user, this.pass)

      conn
    }

    // INSERT/UPDATE/DELETE
    private def executeUpdate(query: String): Int = {
      var statement: Statement = null
      var retry = 0
      var qryResult = 0
      var runCmd = true
      while (runCmd && retry < maxRetry) {
        runCmd = false
        try {
          val connection = getConnection()
          // If we failed to get connection, we are let it failing here
          statement = connection.createStatement()
          qryResult = statement.executeUpdate(query)
        } catch {
          case ex: Throwable => {
            closeStatement(statement)
            statement = null
            retry = retry + 1
            // if it is continueously failing, reopening the connection
            if ((retry % 5) == 0)
              close
            Log.logError("Exception while executing query: %s \nRetry: %2d/%2d".format(query, retry, maxRetry), ex)
            runCmd = true
          }
        } finally {
          closeStatement(statement)
          statement = null
        }
      }
      qryResult
    }

    // SELECT
    private def executeQuery(query: String): (ResultSet, Statement) = {
      var statement: Statement = null
      var retry = 0
      var resultSet: ResultSet = null
      while (resultSet == null && retry < maxRetry) {
        try {
          val connection = getConnection()
          // If we failed to get connection, we are let it failing here
          statement = connection.createStatement()
          resultSet = statement.executeQuery(query)
        } catch {
          case ex: Throwable => {
            closeResultSet(resultSet)
            resultSet = null
            closeStatement(statement)
            statement = null

            retry = retry + 1
            // if it is continueously failing, reopening the connection
            if ((retry % 5) == 0)
              close
            Log.logError("Exception while executing query: %s \nRetry: %2d/%2d".format(query, retry, maxRetry), ex)
          }
        }
      }
      (resultSet, statement)
    }

    def closeResultSet(resultSet: ResultSet): Unit = {
      try {
        if (resultSet != null)
          resultSet.close()
      } catch {
        case e: Throwable => {
          Log.logError("Failed to close ResultSet", e)
        }
      }
    }

    def closeStatement(statement: Statement): Unit = {
      try {
        if (statement != null)
          statement.close()
      } catch {
        case e: Throwable => {
          Log.logError("Failed to close Statement", e)
        }
      }
    }

    def close() {
      if (conn != null && !conn.isClosed)
        conn.close()
      conn = null
    }
  }

  case class ExecQueues(stage1Queue: ExecutorService, stage1QSize: AtomicInteger, availJdbcMgrs: MuSet[JDBCManager])

  case class ShutdownInfo(var isShutdown: Boolean)

  val cs_other01_step1_presto_del_qry: String =
    """
      #delete from flare_8.CustomerSubject_Other01 where tbl_dt = ${END_DATE} and aggr='daily'
      #
      """.stripMargin('#')

  val cs_other01_step1_presto_ins_qry: String =
    """
      #INSERT INTO flare_8.CustomerSubject_Other01 
      #select 
      #COALESCE(val_seg_tbl.msisdn_key, sub_type_tariff_typ_tbl.msisdn_key) as msisdn_key,
      #COALESCE(val_seg_tbl.cmvs_cvs, 0.0) as cmvs_cvs,
      #COALESCE(sub_type_tariff_typ_tbl.service_class_id, 'UNKNOWN') as service_class_id, 
      #COALESCE(sub_type_tariff_typ_tbl.subscriber_type, 2) as subscriber_type, 
      #COALESCE(sub_type_tariff_typ_tbl.tariff_type, 'UNKNOWN') as tariff_type, 
      #COALESCE(sub_type_tariff_typ_tbl.service_class_name, 'UNKNOWN') as service_class_name,
      #'daily' as aggr,
      #${END_DATE} as tbl_dt
      #from
      #(
      #select msisdn_key, cmvs_cvs 
      #from flare_8.CustomerSubject_ValSeg
      #where aggr = 'daily' and tbl_dt = ${END_DATE}
      #) as val_seg_tbl
      #full outer join
      #(
      #select service_class_id_tbl.msisdn_key, service_class_id_tbl.service_class_id, COALESCE(package_tbl.subscriber_type, 2) as subscriber_type, 
      #COALESCE(package_tbl.tariff_type, 'UNKNOWN') as tariff_type, COALESCE(package_tbl.service_class_name, 'UNKNOWN') as service_class_name
      #from
      #(select a.msisdn_key, a.service_class_id, a.tbl_dt
      #from
      #(select msisdn_key, service_class_id, tbl_dt
      #from flare_8.sdp_dmp_ma
      #where tbl_dt between ${START_DATE} and ${END_DATE}) a
      #inner join
      #(
      #select msisdn_key,  max(cast(tbl_dt as varchar) || '-' || (case when service_class_id in ('58', '59') then '1-' when service_class_id in ('70', '71') then '0-' else '2-' end) || service_class_id) as max_tbl_dt_service_class_id
      #from flare_8.sdp_dmp_ma
      #where tbl_dt between ${START_DATE} and ${END_DATE}
      #group by msisdn_key
      #) b
      #on (a.msisdn_key = b.msisdn_key and (cast(a.tbl_dt as varchar) || '-' || (case when a.service_class_id in ('58', '59') then '1-' when a.service_class_id in ('70', '71') then '0-' else '2-' end) || a.service_class_id) = b.max_tbl_dt_service_class_id)
      #group by a.msisdn_key, a.service_class_id, a.tbl_dt
      #) service_class_id_tbl
      #left join
      #(
      #select 
      #tbl_dt, package_cd as service_class_id, package_descr_txt as service_class_name, package_group,
      #case 
      #when package_group = 'PREPAID' then 0
      #when package_group = 'POSTPAID' then 1
      #when package_group = 'NONGSM' then 3
      #when package_group = 'HYBRID' then 4
      #else 2
      #end subscriber_type,
      #package_cd || ' - ' || package_descr_txt as tariff_type
      #from flare_8.dim_package_ng where tbl_dt=20180702
      #) package_tbl
      #on (service_class_id_tbl.service_class_id = package_tbl.service_class_id)
      #) as sub_type_tariff_typ_tbl
      #on (val_seg_tbl.msisdn_key = sub_type_tariff_typ_tbl.msisdn_key)
      #
      #
      #
      """.stripMargin('#')

  private def getGMTCalendarFromYYYYMMDD(ts: Int): Calendar = {
    var cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    cal.setTimeInMillis(0)
    cal.set(ts/10000, ((ts%10000)/100) - 1, ts%100)
    cal
  }

  private def dateAdd_YYYYMMDD(dt: Int, dateOffset: Int): Int = {
    try {
      val cal = getGMTCalendarFromYYYYMMDD(dt)
      cal.add(Calendar.DATE, dateOffset);
      return (cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH) + 1) * 100 + cal.get(Calendar.DAY_OF_MONTH))
    } catch {
      case e1: Throwable => {
        throw e1
      }
    }
  }

  private def replaceKeyValues(template: String, keyValue: Map[String, String]): String = {
    var _template = template

    keyValue.foreach(kv => {
      _template = _template.replace(kv._1, kv._2)
    })
    _template
  }
  
  private def addInputStreamThreadPool(pool: ExecutorService, proc: Process, bufferReads: MuArray[BufferedReader], is: InputStream, sb: StringBuilder = null): Unit = {
    try {
      pool.execute(new Runnable() {
        val inputStream = is

        override def run(): Unit = {
          var bufferedReader: BufferedReader = null
          try {
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
            bufferReads += bufferedReader
            var line: String = null
            line = bufferedReader.readLine
            while (!pool.isShutdown && !pool.isTerminated && proc.isAlive && line != null) {
              Log.logMsg(line)
              if (sb != null)
                sb.append(line).append("\n")
              line = bufferedReader.readLine
            }
          }
          catch {
            case e: Throwable => Log.logError("", e)
          } finally {
            if (bufferedReader != null)
              bufferedReader.close()
          }
        }
      })
    } catch {
      case e: Throwable => Log.logError("", e)
    }
  }

  private def runSysCmd(cmdPrefixStr: String, argsList: Seq[String], outputSb: StringBuilder = null, errorSb: StringBuilder = null, timeOut: Long = 120, maxRetry: Int = 1): Int = {
    var procRetCode = -1000
    val mRetry = if (maxRetry > 0) maxRetry else 1
    var retry = 0
    var isFinished = false
    var process: Process = null
    try {
      val pool: ExecutorService = Executors.newFixedThreadPool(2)
      val bufferReaders = MuArray[BufferedReader]()
      while (retry <= mRetry && !isFinished) {
        Log.logMsg("%s executing command: %s".format(cmdPrefixStr, argsList.mkString(" ")))
        try {
          val procBuilder = new java.lang.ProcessBuilder(argsList: _*)
          process = procBuilder.start()
          addInputStreamThreadPool(pool, process, bufferReaders, process.getErrorStream, errorSb)
          addInputStreamThreadPool(pool, process, bufferReaders, process.getInputStream, outputSb)
          isFinished = process.waitFor(timeOut, TimeUnit.MINUTES)
          procRetCode = process.exitValue()
        }
        catch {
          case e: Throwable =>
            try {
              retry = retry + 1
              val msg = "Timeout when executed command: %s .Timeout is %2d minutes. retry:%2d/%2d".format(argsList.mkString(" "), timeOut, retry, maxRetry)
              Log.logError(msg, e)
              errorSb.append(e.getMessage)
              process.destroyForcibly()
            } catch {
              case ex: Throwable => Log.logError("", ex)
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
          case e: Throwable => Log.logError("", e)
        }
      })
    } catch {
      case e: Throwable => Log.logError("", e)
    }
    procRetCode
  }

  private val gmtTimezone = TimeZone.getTimeZone("GMT")

  private def getCalendarFromYYYYMMDD(v: Int) : Calendar = {
    val yyyy = v / 10000
    val mm = (v % 10000) / 100
    val dd = (v % 100)
    var newcal: Calendar = Calendar.getInstance(gmtTimezone);
    newcal.setTimeInMillis(0)
    newcal.set(yyyy, mm - 1, dd)
    newcal
  }

  private def getYYYYMMDDFromCalendar(cal: Calendar) : Int = {
    cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH)+1) * 100 + cal.get(Calendar.DAY_OF_MONTH)
  }

  private def getYYYYMM01FromCalendar(cal: Calendar) : Int = {
    cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH)+1) * 100 + 1
  }

  def getRollingMonthValSegStartDate(date_key: Int): Int = {
    val cal = getCalendarFromYYYYMMDD(date_key)
    cal.add(Calendar.MONTH, -3);
    cal.add(Calendar.DATE, 1)
    getYYYYMMDDFromCalendar(cal)
  }

  def getCalendarMonthValSegStartDate(date_key: Int): Int = {
    val cal = getCalendarFromYYYYMMDD(date_key)
    cal.add(Calendar.DATE, 1)
    cal.add(Calendar.MONTH, -3);
    getYYYYMM01FromCalendar(cal)
  }

  def getCalendarMonthValSegEndDate(date_key: Int): Int = {
    val cal = getCalendarFromYYYYMMDD(date_key)
    cal.add(Calendar.DATE, 1)
    val day = cal.get(Calendar.DAY_OF_MONTH)
    if (day == 1)
      cal.add(Calendar.DATE, -1);
    else
      cal.add(Calendar.DATE, -1 * cal.get(Calendar.DAY_OF_MONTH));
    getYYYYMMDDFromCalendar(cal)
  }
  
  var nextKinit: Long = 0
  
  private def runKinit(cmdPrefixStr: String): Unit = {
    val curTime = System.currentTimeMillis()
    if (curTime < nextKinit) {
      Log.logMsg("%s ignored kinit executing command".format(cmdPrefixStr))
      return
    }
    nextKinit = curTime + 3600 * 1000 // Once every hr
    try {
      val kinit: String = "kinit -k -t /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM"
      runSysCmd(cmdPrefixStr, kinit.split(" ").toSeq, errorSb = null)
    } catch {
      case e: Throwable => Log.logError("", e)
    }
  }

  private def getJDBCManager(execQs: ExecQueues): JDBCManager = execQs.synchronized {
    try {
      val jdbcManager = execQs.availJdbcMgrs.head
      execQs.availJdbcMgrs -= jdbcManager
      jdbcManager
    } catch {
      case e: Throwable => {
        null
      }
    }
  }

  private def returnJDBCManager(execQs: ExecQueues, jdbcManager: JDBCManager): Unit = execQs.synchronized {
    if (jdbcManager == null)
      return
    execQs.availJdbcMgrs += jdbcManager
  }

  private def runStage1(execQs: ExecQueues, date: Int): Unit = {
    var jdbcManager: JDBCManager = null
    try {
      val s = System.currentTimeMillis()
      jdbcManager = getJDBCManager(execQs)

      val cmdPrefixStr = "Day:%d => Step1".format(date)
      runKinit(cmdPrefixStr + "C1")

      val t1 = System.currentTimeMillis()
      
      val startDate = dateAdd_YYYYMMDD(date, extraSdpPrevDays * -1)

      val delSql = replaceKeyValues(cs_other01_step1_presto_del_qry, Map("${END_DATE}" -> date.toString))
      // Log.logMsg("Step1 DelQry:\n %s \n\n".format(delSql))
      jdbcManager.runDeleteOrInsertQuery(cmdPrefixStr + "C2", delSql)

      val t2 = System.currentTimeMillis()
      val insSql = replaceKeyValues(cs_other01_step1_presto_ins_qry, Map("${START_DATE}" -> startDate.toString, "${END_DATE}" -> date.toString))

      // Log.logMsg("Step1 InsQry:\n %s \n\n".format(insSql))
      jdbcManager.runDeleteOrInsertQuery(cmdPrefixStr + "C3", insSql)

      val e = System.currentTimeMillis()
      Log.logMsg("Day:%d => Step1 took %dms (C1:%d, C2:%d, C3:%d)".format(date, (e - s), (t1 - s), (t2 - t1), (e - t2)))
    } catch {
      case e: Throwable => {
        Log.logError("", e)
      }
    } finally {
      returnJDBCManager(execQs, jdbcManager)
    }

  }

  private def addTaskToThreadPool(pool: ExecutorService, curDt: Int, stageVal: StageNumbers.Value, execQs: ExecQueues): Unit = {
    try {
      if (stageVal == StageNumbers.stageONE)
        execQs.stage1QSize.incrementAndGet()

      pool.execute(new Runnable() {
        val date = curDt
        val stage = stageVal
        val execQueues = execQs

        override def run(): Unit = {
          try {
            if (stage == StageNumbers.stageONE) {
              runStage1(execQueues, date)
            }
          }
          catch {
            case e: Throwable => Log.logError("", e)
          } finally {
            if (stage == StageNumbers.stageONE)
              execQs.stage1QSize.decrementAndGet()
          }
        }
      })
    } catch {
      case e: Throwable => Log.logError("", e)
    }
  }

  private def populateDateRange(startRange: Int, endRange: Int, stepVal: Int, queue: ExecutorService, stage: StageNumbers.Value, execQs: ExecQueues): Unit = {
    var curDt = startRange
    while ((stepVal < 0 && curDt >= endRange) || (stepVal > 0 && curDt <= endRange)) {
      try {
        addTaskToThreadPool(queue, curDt, stage, execQs)
      } catch {
        case e: Throwable => {}
      }

      curDt = dateAdd_YYYYMMDD(curDt, stepVal)
    }
  }

  private def populateDateRangeFromString(dateRanges: String, stepVal: Int, queue: ExecutorService, stage: StageNumbers.Value, execQs: ExecQueues): Unit = {
    dateRanges.split(",").map(dateRange => dateRange.trim).filter(dateRange => !dateRange.isEmpty).foreach(dateRange => {
      val rng = dateRange.split("-").map(dt => dt.trim).filter(dt => !dt.isEmpty)
      if (rng.length > 0) {
        val startRng = rng(0).toInt
        var endRng = startRng
        if (rng.length > 1)
          endRng = rng(1).toInt
        populateDateRange(startRng, endRng, stepVal, queue, stage, execQs)
      } 
    })
  }

  private def waitForQueueFinish(execQs: ExecQueues, qSize: AtomicInteger, shutdownInfo: ShutdownInfo): Unit = {
    // Wait for empty queues in the order
    while (qSize.get > 0 && !shutdownInfo.isShutdown) {
      try {
        Thread.sleep(1000)
        Log.logMsg("Outstanding tasks in queues. Stage1QSize:%d".format(execQs.stage1QSize))
      } catch {
        case e: InterruptedException => {
          shutdownInfo.isShutdown = true
        }
        case e: Throwable => {}
      }
    }
  }

  private def waitForQueueFinish(execQs: ExecQueues, stageQueue: ExecutorService, shutdownInfo: ShutdownInfo): Unit = {
    // Wait for empty queues in the order
    while (!stageQueue.isTerminated && !shutdownInfo.isShutdown) {
      try {
        Thread.sleep(1000)
        Log.logMsg("Outstanding tasks in queues. Stage1QSize:%d".format(execQs.stage1QSize))
      } catch {
        case e: InterruptedException => {
          shutdownInfo.isShutdown = true
        }
        case e: Throwable => {}
      }
    }
  }

def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--stage1To2DateRanges", argStartDate: String) => stage1_Date = argStartDate.trim
      }
    else
      println("Please pass stage1date range: example--> \"20180701-20180705\" ")
  }

var stage1_Date=""

  def main(args: Array[String]): Unit = {
//    val stage1DateRanges="20180813-20180816"
 val stage1DateRanges=stage1_Date
    val stage1StepVal = 1

    val stage1ParallelTasks = 4

    val stage1Queue = Executors.newFixedThreadPool(stage1ParallelTasks)

    val shutdownInfo = ShutdownInfo(false)

    val tmpJdbcMgrs: Array[JDBCManager] = (0 until (stage1ParallelTasks + 7)).toArray.map(v => new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null, shutdownInfo))
    val availJdbcMgrs = MuSet[JDBCManager]()
    availJdbcMgrs ++= tmpJdbcMgrs

    val execQs = ExecQueues(stage1Queue, new AtomicInteger, availJdbcMgrs)

    // Populate queues
    Log.logMsg("Populating & Start Processing using stage1DateRanges:%s with stage1StepVal:%d".format(stage1DateRanges, stage1StepVal))

    populateDateRangeFromString(stage1DateRanges, stage1StepVal, stage1Queue, StageNumbers.stageONE, execQs)

    waitForQueueFinish(execQs, execQs.stage1QSize, shutdownInfo)

    execQs.stage1Queue.shutdown()

    waitForQueueFinish(execQs, execQs.stage1Queue, shutdownInfo)

    tmpJdbcMgrs.foreach(v => {
      try {
        if (v != null)
          v.close()
      } catch {
        case e: Throwable => {}
      }
    })

    Log.logMsg("All tasks are done")
  }
}

