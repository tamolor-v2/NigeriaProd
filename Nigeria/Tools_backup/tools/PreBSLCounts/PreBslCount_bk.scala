#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#
import java.io._
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.sys.process._
object PreBSLCount {
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val timeStampFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
  var listOfFeedsStats=ListBuffer[(String,Long,Long)]()
  var listOfFeeds:ListBuffer[(String,Long,Long)]=ListBuffer(
    ("flare_8.bundle4u_gprs",0,0),
    ("flare_8.bundle4u_voice",0,0),
    ("flare_8.cb_serv_mast_view",0,0),
    ("flare_8.cs5_air_adj_ma",0,0),
    ("flare_8.cs5_air_refill_ma",0,0),
    ("flare_8.cs5_ccn_gprs_ma",0,0),
    ("flare_8.cs5_ccn_sms_ma",0,0),
    ("flare_8.cs5_ccn_voice_ma",0,0),
    ("flare_8.cs5_sdp_acc_adj_ma",0,0),
    ("flare_8.cug_access_fees",0,0),
    ("flare_8.dmc_dump_all",0,0),
    ("flare_8.ggsn_cdr",0,0),
    ("flare_8.mobile_money",0,0),
    ("flare_8.msc_cdr",0,0),
    ("flare_8.sdp_dmp_ma",0,0),
    ("flare_8.wbs_pm_rated_cdrs",0,0))
  val Query="""select count(*),count(distinct file_name)  cn from {feed} where tbl_dt={tbl_dt}"""

  var date=0
var noDays=0
  def main(args: Array[String]): Unit = {
    val pool = Executors.newFixedThreadPool(1)
    Log.logMsg("start")
        parseArgs(args)
Seq("hadoop", "fs","-rm","/FlareData/output_8/PRE_BSL_COUNTS/tbl_dt="+date+"/*")
println("hadoop fs -rm -r /FlareData/output_8/PRE_BSL_COUNTS/tbl_dt="+date+"/*")

    val today= LocalDate.now.format(formatter).toInt
    val runTimeStamp= LocalDateTime.now.format(timeStampFormatter)
    //    val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive/flare_8", "test", null)

    val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)

      Log.logMsg("processing date: "+date)
//      Seq("hive", "-e",  "\"drop table if exists flare_8.lea_mapping_temp\"") !;
    for(i<-listOfFeeds.indices) {
     val query= Query.replace("""{tbl_dt}""", date.toString).replace("""{feed}""",listOfFeeds(i)._1)
      val table_name=query.substring(query.indexOf("flare_8")-1,query.indexOf("where")-1)
      println(Query.replace("""{tbl_dt}""", date.toString).replace("""{feed}""",listOfFeeds(i)._1))
      //listOfFeedsStats+=((listOfFeeds(i)._1,100))
      val (rs,fcnt)=jDBCManager.executeSelectQuery(Query.replace("""{tbl_dt}""", date.toString).replace("""{feed}""",listOfFeeds(i)._1),table_name)
      listOfFeedsStats+=((listOfFeeds(i)._1,rs.toLong,fcnt.toLong))
//      Seq("hive","S", "-e", "\"set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table flare_8.lea_mapping_final partition(tbl_dt) select * from flare_8.lea_mapping_temp;") !;
//      date = LocalDate.parse(date.toString).minusDays(1).format(formatter).toInt
    }
    println(date)
    println(runTimeStamp)
    listOfFeedsStats.foreach(println)
//insert into table flare_8.pre_bsl_counts partition(tbl_dt) values("gprs_ma",1000,"20181019141515",20180909);
var full_insert=""
//Seq("hive", "-e","alter table flare_8.pre_bsl_counts set tblproperties('EXTERNAL'='FALSE');")
//Seq("hive", "-e","truncate table t partition (tbl_dt="+date+");")
//Seq("hive", "-e","alter table flare_8.pre_bsl_counts set tblproperties('EXTERNAL'='TRUE');")
Seq("hadoop", "fs","-rm","-r","/FlareData/output_8/PRE_BSL_COUNTS/tbl_dt="+date+"/*")!;
for(feed<-listOfFeedsStats){
full_insert=full_insert+ "("+"\""+feed._1+"\","+feed._2+","+feed._3+",\""+runTimeStamp+"\","+date+"),"
}
full_insert=full_insert.dropRight(1)
 Log.logMsg("Inserting counts into table flare_8.pre_bsl_counts")
println(full_insert)
Seq("hive", "-e", "set hive.exec.dynamic.partition.mode=nonstrict;insert into table flare_8.pre_bsl_counts partition(tbl_dt) values"+full_insert)!
//for(feed<-listOfFeedsStats){
// Log.logMsg("inserting"+ feed._1+","+feed._2+","+runTimeStamp+","+date)
//Seq("hive", "-e", "set hive.exec.dynamic.partition.mode=nonstrict;insert into table flare_8.pre_bsl_counts partition(tbl_dt) values(\""+feed._1+"\","+feed._2+",\""+runTimeStamp+"\","+date+")")!;
//}

//for(feed<-listOfFeedsStats){
// Log.logMsg("inserting"+ feed._1+","+feed._2+","+runTimeStamp+","+date)
//jDBCManager.executeQuery("insert into  flare_8.pre_bsl_counts values("+"\'"+feed._1+"\',"+feed._2+",\'"+runTimeStamp+"\',"+date+")")
//}

    Log.logMsg("done")
//    jDBCManager.close
  }

  def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--date", argDataDate: String) => date = argDataDate.toInt
        case Array("--noDays", argDataDate: String) => noDays = argDataDate.toInt

      }
    else
      Log.logError("No date is supplied", false)
  }
}


// /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar



class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

  private val maxRetry =1
  def executeSelectQuery(query: String,table_name:String):(String,String) = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    //    Log.logMsg(query)
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("generating table statement for: "+table_name)
          statement = conn.createStatement()
          Log.logMsg("executing statementfor :"+ table_name)
          val result = statement.executeQuery(query)
          Log.logMsg("successfully executed statement for "+table_name)
          val cnt=if(result.next) result.getString(1) else ""
	val fcnt= result.getString(2) 
          return (cnt,fcnt)
        }
      } catch {
        case ex: Throwable => {
          Log.logMsg("reprocessing "+table_name)
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          ex.printStackTrace
        }
      }
    }
        return ("","")
    //(resultSet, statement)
  }

  def executeQuery(query: String)  = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    var status="Start"
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          statement = conn.createStatement()
          val result = statement.executeUpdate(query)

        }
      } catch {
        case ex: Throwable => {
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          ex.printStackTrace
        }
      }
    }
    //(resultSet, statement)
  }

  def close() {
    if (conn != null && !conn.isClosed)
      conn.close()
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
PreBSLCount.main(args)

