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
    val stageNONE, stageONE, stageTWO = Value
  }

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

  case class ExecQueues(stage1Queue: ExecutorService, stage2Queue: ExecutorService, stage1QSize: AtomicInteger, stage2QSize: AtomicInteger, availJdbcMgrs: MuSet[JDBCManager])

  case class ShutdownInfo(var isShutdown: Boolean)

  val val_seg_step1_presto_del_qry: String =
    """
      #delete from flare_8.CustomerSubject_ValSeg_Stage1 where tbl_dt = ${RM_END_DATE} and aggr='daily'
      #
      """.stripMargin('#')

  val val_seg_step1_presto_ins_qry: String =
    """
      #insert into flare_8.CustomerSubject_ValSeg_Stage1
      #select 
      #a.msisdn_key as msisdn_key,
      #b.val_seg_tenure_months,
      #b.cal_mon_val_seg_rev_mon_divisor,
      #b.rol_mon_val_seg_rev_mon_divisor,
      #sum(case when a.tbl_dt >= b.cm_valid_data_start_dt and a.tbl_dt <= b.cm_end_dt and a.total_revenue < 10000000 then a.total_revenue else 0.0 end) as cm_val_seg_total_revenue,
      #sum(case when a.tbl_dt >= b.rm_valid_data_start_dt and a.tbl_dt <= b.rm_end_dt and a.total_revenue < 10000000 then a.total_revenue else 0.0 end) as rm_val_seg_total_revenue,
      #'daily' as aggr,
      #${RM_END_DATE} as tbl_dt
      #from 
      #(
      #select 
      #tbl_dt,
      #msisdn_key,
      #(COALESCE(rev_voice_onnet, 0.0) + COALESCE(rev_voice_offnet, 0.0) + COALESCE(rev_voice_int, 0.0) + COALESCE(rev_sms_onnet, 0.0) + COALESCE(rev_sms_offnet, 0.0) + COALESCE(rev_sms_int, 0.0) + COALESCE(rev_data_payg, 0.0) + COALESCE(rev_vas1, 0.0) + COALESCE(rev_vas2, 0.0) + COALESCE(rev_vas3, 0.0) + COALESCE(rev_vas4, 0.0) + COALESCE(rev_other_vas, 0.0) + COALESCE(tot_rev_in, 0.0) + COALESCE(rev_voice_roam_outgoing, 0.0) + COALESCE(customer_sys_01, 0.0) + COALESCE(customer_sys_03, 0.0)) as total_revenue
      #from flare_8.CustomerSubject
      #where tbl_dt between ${CM_START_DATE} and ${RM_END_DATE} and aggr='daily'
      #) a
      #inner join
      #(
      #select 
      #cm_end_dt,
      #rm_end_dt,
      #msisdn_key,
      #val_seg_tenure_months,
      #cm_valid_data_start_dt,
      #rm_valid_data_start_dt,
      #(case when (cal_mon_val_seg_data_months <= 0 or max_cm_activation_dt <= 0 or cal_mon_val_seg_data_months >= 3) then 3 when (cal_mon_val_seg_data_months >= 2) then 2 else 1 end) as cal_mon_val_seg_rev_mon_divisor,
      #(case when (rol_mon_val_seg_data_months <= 0 or max_rm_activation_dt <= 0 or rol_mon_val_seg_data_months >= 3) then 3 when (rol_mon_val_seg_data_months >= 2) then 2 else 1 end) as rol_mon_val_seg_rev_mon_divisor,
      #in_today_sdp_val
      #from
      #(
      #select 
      #cm_end_dt,
      #rm_end_dt,
      #msisdn_key,
      #
      #(date_diff('month', date_parse(cast (IF (max_cm_activation_dt < 20000101, 20000101, max_cm_activation_dt) as varchar), '%Y%m%d'), date_parse(cast (cm_end_dt as varchar), '%Y%m%d')) + 1) as cal_mon_val_seg_data_months,
      #(date_diff('month', date_parse(cast (IF (max_rm_activation_dt < 20000101, 20000101, max_rm_activation_dt) as varchar), '%Y%m%d'), date_parse(cast (rm_end_dt as varchar), '%Y%m%d')) + 1) as rol_mon_val_seg_data_months,
      #
      #((${RM_END_DATE} / 10000 - IF (max_rm_activation_dt < 20000101, 20000101, max_rm_activation_dt) / 10000) * 12 + ((${RM_END_DATE} % 10000)/100 - (IF (max_rm_activation_dt < 20000101, 20000101, max_rm_activation_dt)  % 10000)/100)) as val_seg_tenure_months,
      #
      #max_cm_activation_dt,
      #max_rm_activation_dt,
      #IF (cm_start_dt > max_cm_activation_dt, cm_start_dt, max_cm_activation_dt) as cm_valid_data_start_dt,
      #IF (rm_start_dt > max_rm_activation_dt, rm_start_dt, max_rm_activation_dt) as rm_valid_data_start_dt,
      #in_today_sdp_val
      #from (
      #select 
      #${CM_START_DATE} as cm_start_dt,
      #${CM_END_DATE} as cm_end_dt,
      #${RM_START_DATE} as rm_start_dt,
      #${RM_END_DATE} as rm_end_dt,
      #msisdn_key, 
      #max(IF ((tbl_dt BETWEEN ${CM_START_DATE} and ${CM_END_DATE}), (COALESCE(activation_date, 0)), 0)) as max_cm_activation_dt,
      #max(IF ((tbl_dt BETWEEN ${RM_START_DATE} and ${RM_END_DATE}), (COALESCE(activation_date, 0)), 0)) as max_rm_activation_dt,
      #sum(IF ((tbl_dt = ${RM_END_DATE} and is_in_today_sdp), 1, 0)) as in_today_sdp_val,
      #sum(IF ((tbl_dt = ${RM_END_DATE} and is_in_today_sdp), 1000000 + dola, 0)) as last_day_dola_val
      #from flare_8.CustomerSubject
      #where tbl_dt between ${CM_START_DATE} and ${RM_END_DATE} and aggr='daily'
      #group by msisdn_key
      #) tmp1
      #where (last_day_dola_val - 1000000) between 0 and 89
      #) tmp2
      #) b
      #on (a.msisdn_key = b.msisdn_key and b.in_today_sdp_val > 0)
      #group by a.msisdn_key, 
      #b.val_seg_tenure_months,
      #b.cal_mon_val_seg_rev_mon_divisor,
      #b.rol_mon_val_seg_rev_mon_divisor
      #
      #
      """.stripMargin('#')

  val val_seg_step2_presto_del_qry: String =
    """
      #delete from flare_8.CustomerSubject_ValSeg where tbl_dt = ${RM_END_DATE} and aggr='daily'
      #
      """.stripMargin('#')

  val val_seg_step2_presto_ins_qry: String =
    """
      #insert into flare_8.CustomerSubject_ValSeg
      #SELECT
      #msisdn_key,
      #val_seg_tenure_months,
      #vs_customer_relative_tenure,
      #cm_val_seg_total_revenue,
      #cal_mon_val_seg_rev_mon_divisor,
      #cm_vs_avg_rev_mon as cm_vs_arpu,
      #cm_vs_customer_relative_arpu,
      #(((0.60000000000*cm_vs_customer_relative_arpu)+(0.40000000000*vs_customer_relative_tenure))/(select count(*) from flare_8.CustomerSubject_ValSeg_Stage1 where aggr = 'daily' and tbl_dt = ${RM_END_DATE})) cmvs_cvs,
      #
      #rm_val_seg_total_revenue,
      #rol_mon_val_seg_rev_mon_divisor,
      #rm_vs_avg_rev_mon as rm_vs_arpu,
      #rm_vs_customer_relative_arpu,
      #(((0.60000000000*rm_vs_customer_relative_arpu)+(0.40000000000*vs_customer_relative_tenure))/(select count(*) from flare_8.CustomerSubject_ValSeg_Stage1 where aggr = 'daily' and tbl_dt = ${RM_END_DATE})) rmvs_cvs,
      #aggr,
      #tbl_dt
      #
      #from (
      #SELECT
      #msisdn_key,
      #cm_val_seg_total_revenue,
      #cal_mon_val_seg_rev_mon_divisor,
      #rm_val_seg_total_revenue,
      #rol_mon_val_seg_rev_mon_divisor,
      #val_seg_tenure_months,
      #(cm_val_seg_total_revenue / cal_mon_val_seg_rev_mon_divisor) as cm_vs_avg_rev_mon,
      #(rm_val_seg_total_revenue / rol_mon_val_seg_rev_mon_divisor) as rm_vs_avg_rev_mon,
      #cast (rank() over (order by (cm_val_seg_total_revenue / cal_mon_val_seg_rev_mon_divisor)) as int) as cm_vs_customer_relative_arpu,
      #cast (rank() over (order by val_seg_tenure_months) as int) as vs_customer_relative_tenure,
      #cast (rank() over (order by (rm_val_seg_total_revenue / rol_mon_val_seg_rev_mon_divisor)) as int) as rm_vs_customer_relative_arpu,
      #aggr,
      #tbl_dt
      #from flare_8.CustomerSubject_ValSeg_Stage1 where aggr = 'daily' and tbl_dt = ${RM_END_DATE}
      #)
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
      val rol_mon_start_dt = getRollingMonthValSegStartDate(date)
      val cal_mon_start_dt = getCalendarMonthValSegStartDate(date)
      val cal_mon_end_dt = getCalendarMonthValSegEndDate(date)

      val delSql = replaceKeyValues(val_seg_step1_presto_del_qry, Map("${RM_END_DATE}" -> date.toString))
      // Log.logMsg("Step1 DelQry:\n %s \n\n".format(delSql))
      jdbcManager.runDeleteOrInsertQuery(cmdPrefixStr + "C2", delSql)

      val t2 = System.currentTimeMillis()
      val insSql = replaceKeyValues(val_seg_step1_presto_ins_qry, Map("${CM_START_DATE}" -> cal_mon_start_dt.toString, "${CM_END_DATE}" -> cal_mon_end_dt.toString, "${RM_START_DATE}" -> rol_mon_start_dt.toString, "${RM_END_DATE}" -> date.toString))
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

  private def runStage2(execQs: ExecQueues, date: Int): Unit = {
    var jdbcManager: JDBCManager = null
    try {
      val s = System.currentTimeMillis()
      jdbcManager = getJDBCManager(execQs)

      val cmdPrefixStr = "Day:%d => Step2".format(date)
      runKinit(cmdPrefixStr + "C1")

      val t1 = System.currentTimeMillis()
      val delSql = replaceKeyValues(val_seg_step2_presto_del_qry, Map("${RM_END_DATE}" -> date.toString))
      // Log.logMsg("Step2 DelQry:\n %s \n\n".format(delSql))
      jdbcManager.runDeleteOrInsertQuery(cmdPrefixStr + "C2", delSql)

      val t2 = System.currentTimeMillis()
      val insSql = replaceKeyValues(val_seg_step2_presto_ins_qry, Map("${RM_END_DATE}" -> date.toString))
      // Log.logMsg("Step2 InsQry:\n %s \n\n".format(insSql))
      jdbcManager.runDeleteOrInsertQuery(cmdPrefixStr + "C3", insSql)

      val e = System.currentTimeMillis()
      Log.logMsg("Day:%d => Step2 took %dms (C1:%d, C2:%d, C3:%d)".format(date, (e - s), (t1 - s), (t2 - t1), (e - t2)))
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
      else if (stageVal == StageNumbers.stageTWO)
        execQs.stage2QSize.incrementAndGet()

      pool.execute(new Runnable() {
        val date = curDt
        val stage = stageVal
        val execQueues = execQs

        override def run(): Unit = {
          try {
            if (stage == StageNumbers.stageONE) {
              runStage1(execQueues, date)
              addTaskToThreadPool(execQs.stage2Queue, curDt, StageNumbers.stageTWO, execQs)
            }
            else if (stage == StageNumbers.stageTWO) {
              runStage2(execQueues, date)
            }
          }
          catch {
            case e: Throwable => Log.logError("", e)
          } finally {
            if (stage == StageNumbers.stageONE)
              execQs.stage1QSize.decrementAndGet()
            else if (stage == StageNumbers.stageTWO)
              execQs.stage2QSize.decrementAndGet()
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
        Log.logMsg("Outstanding tasks in queues. Stage1QSize:%d, Stage2QSize:%d".format(execQs.stage1QSize, execQs.stage2QSize))
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
        Log.logMsg("Outstanding tasks in queues. Stage1QSize:%d, Stage2QSize:%d".format(execQs.stage1QSize, execQs.stage2QSize))
      } catch {
        case e: InterruptedException => {
          shutdownInfo.isShutdown = true
        }
        case e: Throwable => {}
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val stage1To2DateRanges="20180615-20180615"
    val stage2To2DateRanges=""

    val stage1StepVal = 1
    val stage2StepVal = 1

    val stage1ParallelTasks = 1 
    val stage2ParallelTasks = 1

    val stage1Queue = Executors.newFixedThreadPool(stage1ParallelTasks)
    val stage2Queue = Executors.newFixedThreadPool(stage2ParallelTasks)

    val shutdownInfo = ShutdownInfo(false)

    val tmpJdbcMgrs: Array[JDBCManager] = (0 until (stage1ParallelTasks + stage2ParallelTasks + 7)).toArray.map(v => new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null, shutdownInfo))
    val availJdbcMgrs = MuSet[JDBCManager]()
    availJdbcMgrs ++= tmpJdbcMgrs

    val execQs = ExecQueues(stage1Queue, stage2Queue, new AtomicInteger, new AtomicInteger, availJdbcMgrs)

    // Populate queues
    Log.logMsg("Populating & Start Processing Stage 1 & Stage 2 Queues using stage1To2DateRanges:%s, stage2To2DateRanges:%s with stage1StepVal:%d, stage2StepVal:%d".format(stage1To2DateRanges, stage2To2DateRanges, stage1StepVal, stage2StepVal))

    populateDateRangeFromString(stage2To2DateRanges, stage2StepVal, stage2Queue, StageNumbers.stageTWO, execQs)
    populateDateRangeFromString(stage1To2DateRanges, stage1StepVal, stage1Queue, StageNumbers.stageONE, execQs)

    waitForQueueFinish(execQs, execQs.stage1QSize, shutdownInfo)
    waitForQueueFinish(execQs, execQs.stage2QSize, shutdownInfo)

    execQs.stage1Queue.shutdown()
    execQs.stage2Queue.shutdown()

    waitForQueueFinish(execQs, execQs.stage1Queue, shutdownInfo)
    waitForQueueFinish(execQs, execQs.stage2Queue, shutdownInfo)

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

