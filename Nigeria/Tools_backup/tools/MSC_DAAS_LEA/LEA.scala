#!/bin/sh
exec scala -cp /home/daasuser/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#
import java.io._
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.concurrent.{Executors, TimeUnit}

import scala.sys.process._

object LEA {
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")

val Query= """ insert into flare_8.lea_mapping_msc_daas select
               | a.originating_number,
               | cast(a.event_timestamp_enrich*1000 as BIGINT) AS event_timestamp_enrich,
               | a.original_timestamp_enrich AS cdr_ts,
               | a.terminating_number,
               | cast(a.duration as INT) AS call_duration,
               | a.originating_equipment_id imei_originating,
               | a.terminating_equipment_id imei_terminating,
               | a.originating_id imsi_originating,
               | a.terminating_id imsi_terminating,
               | COALESCE(b.base_station_nm,
               | a.originating_station) originating_sitename,
               | COALESCE(cc.base_station_nm,
               | a.terminating_station) terminating_sitename,
               | b.lon originating_longitude,
               | cc.lon terminating_longitude,
               | b.lat originating_latitude,
               | cc.lat terminating_latitude,
               | b.lga originating_lga,
               | cc.lga terminating_lga,
               | b.city originating_city,
               | cc.city terminating_city,
               | b.state originating_state,
               | cc.state terminating_state,
               | calling.calling_subscriber_name AS calling_subscriber_name,
               | called.called_subscriber_name AS called_subscriber_name,
               | a.call_type msc_call_type,
               | case
               | when a.call_type = 'MO' then 'Voice Originating'
               | when a.call_type = 'SMSO' then 'SMS Originating'
               | when a.call_type ='MT' then 'Voice Terminating'
               | when a.call_type = 'SMST' then 'SMS Terminating'
               | else a.call_type end call_type,
               | case
               | when a.call_type in( 'MO','SMSO') then a.originating_number
               | when a.call_type in ('MT','SMST') then a.terminating_number end calling_number,
               | case
               | when a.call_type in ('MO','SMSO') then a.terminating_number
               | when a.call_type in ('MT','SMST') then a.originating_number end called_number,
               | a.tbl_dt
               | FROM flare_8.msc_daas a left outer JOIN flare_8.cell_cgi_mapping b ON (a.originating_station=b.cgi)
               | left outer JOIN flare_8.cell_cgi_mapping cc ON (a.terminating_station=cc.cgi)
               | LEFT OUTER JOIN (select upper(substring(first_name,1,1)) || lower(substring(first_name,2)) || ' ' || upper(substring(last_name,1,1)) || lower(substring(last_name,2)) calling_subscriber_name,msisdn_key
               | from flare_8.newreg_bioupdt_pool where tbl_dt={tbl_dt})  calling ON (cast(a.originating_number as bigint)=calling.msisdn_key)
               | LEFT OUTER JOIN (select upper(substring(first_name,1,1)) || lower(substring(first_name,2)) || ' ' || upper(substring(last_name,1,1)) || lower(substring(last_name,2)) called_subscriber_name,msisdn_key
               | from flare_8.newreg_bioupdt_pool where tbl_dt={tbl_dt}) called ON (cast(a.terminating_number as bigint)=called.msisdn_key) WHERE a.tbl_dt={tbl_dt}
             """.stripMargin
	  var failedTablesList=scala.collection.mutable.Map[String,String]()
  var successfulTablesList=scala.collection.mutable.Map[String,String]()
  //val filesList=Array("C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectunpack.txt",
  //                    "C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectaggr.txt",
  //                    "C:\\Users\\skhat_000\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectopco.txt",
  //                    "C:\\Users\\skhat_001\\Desktop\\Nigeria\\DPI_BSL\\test\\dpi_bsl_subjectpivot.txt")
  val filesList=Array(
    "/mnt/beegfs/tools/LEA/Queries.hql"
  )
  var date=0
  var noOfDays=1
  def main(args: Array[String]): Unit = {
    val pool = Executors.newFixedThreadPool(1)
    Log.logMsg("start")
    parseArgs(args)
              val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)
//Seq("kinit", "-k", "-t", "/etc/security/keytabs/daasuser.keytab", "daasuser@MTN.COM")!;
//Seq("hadoop", "fs", "-mkdir", "/FlareData/output_8/LEA_MAPPING_MSC_DAAS/tbl_dt="+date.toString)!;
Seq("/opt/presto/bin/presto","--server", "master01004:8099", "--catalog", "hive5", "--execute",  "delete from flare_8.lea_mapping_msc_daas where tbl_dt="+date.toString)!;
              for(days<- 1 to noOfDays) {
                Log.logMsg("processing date: "+date)
		Seq("hadoop", "fs", "-rm","-r", "/FlareData/output_8/LEA_MAPPING_MSC_DAAS/tbl_dt="+date.toString)!;
                //Log.logMsg("executing query: \r\n" + Query.replace("""{tbl_dt}""", date.toString))
		 //Seq("/opt/presto/bin/presto","--server","master01004:8099","--catalog","hive5","--execute",  "delete from flare_8.lea_mapping_msc_daas where tbl_dt="+date.toString)!;
                //jDBCManager.executeQuery(Query.replace("""{tbl_dt}""", date.toString))
                Log.logMsg("Running query: " )
		//jDBCManager.prepareTablePartition(date)
		
		jDBCManager.executeQueryOnce(Query.replace("""{tbl_dt}""", date.toString))
		Seq("hadoop", "fs", "-chmod","777", "/FlareData/output_8/LEA_MAPPING_MSC_DAAS/tbl_dt="+date.toString)!;
                //Seq("hive", "-e", "\"set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table FLARE_8.LEA_MAPPING_MSC_DAAS partition(tbl_dt)  select * from flare_8.LEA_Mapping_temp_daas ") !;
                date=LocalDate.parse(date.toString,formatter).minusDays(1).format(formatter).toInt
              }
    Log.logMsg("done")
  }

  def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--date", argDataDate: String) => date = argDataDate.toInt
        case Array("--noDays", argNoDays: String) => noOfDays = argNoDays.toInt
      }
    else
      Log.logError("No date is supplied", true)
  }

  class JDBCManager(private val url: String, private val user: String, private val pass: String) {
    private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

    private val maxRetry = 5

  def prepareTablePartition(tbl_dt:Int):Boolean={
    var statement: Statement = null
    var resultSet: ResultSet = null
    var status="Start"
    Log.logMsg("Checking if temp table exists")
    try {
          Log.logMsg("delete from flare_8.LEA_MAPPING_MSC_DAAS where tbl_dt="+tbl_dt.toString)
          statement.executeUpdate("delete from flare_8.LEA_MAPPING_MSC_DAAS3 where tbl_dt="+tbl_dt.toString)
          return true
        }
     catch {
      case ex: Throwable => {
	Log.logMsg("Error")
        //Log.logMsg(ex.getMessage)
	Log.logMsg("===>"+ex.getStackTrace.toList)
      }
    }
    false
  }

  def executeQueryOnce(query: String) :String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    var result=1
    var status="Start"
    try {
      if (conn != null && !conn.isClosed) {
        statement = conn.createStatement()
        Log.logMsg("executing statement :"+ query)
        result = statement.executeUpdate(query)
        return "Success"
      }
    } catch {
      case ex: Throwable => {
        Log.logMsg("-----------> "+ex.getMessage)
      }
    }
    status="Failed"
    status
  }

    def close() {
      if (conn != null && !conn.isClosed)
        conn.close()
    }
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

class ChunkBreaker {

  def break(fileContents: String): List[String] = {
    fileContents.split("""(?m)\s*(\r?\n\r?\n+)""").map(a=>a.trim).filterNot(_.isEmpty).toList
    //.map { case (a, b, c) => (a, c)}.filterNot(_._2.trim.length == 0)
  }
}
LEA.main(args)



