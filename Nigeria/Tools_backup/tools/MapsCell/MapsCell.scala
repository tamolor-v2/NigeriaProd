#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

import sys.process._
import java.io._
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.Duration
import scala.io.Source
import collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source.fromFile
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.concurrent.{Executors, _}

object MapsCell {
val Query=
  """
    |create table flare_8.mapsinv_cell_tmp AS
    |(
    |(select
    |technology,
    |cell_id,
    |base_station_id,
    |base_station_nm,
    |base_station_region_nm,
    |province_nm,
    |region_nm,
    |sub_region_nm,
    |cluster_nm,
    |site_category_nm,
    |technology_cd,
    |cluster_priority_cd,
    |site_priority_cd,
    |lon,
    |lat,
    |cgi,
    |orig_cgi,
    |tbl_dt
    |from (
    |select
    |'2G' as technology,
    |a.cell_id,
    |a.base_station_id,
    |a.base_station_nm,
    |b.base_station_region_nm base_station_region_nm,
    |a.province_nm,
    |a.region_nm,
    |a.sub_region_nm,
    |a.cluster_nm,
    |a.site_category_nm,
    |a.technology_cd,
    |a.cluster_priority_cd,
    |a.site_priority_cd,
    |a.gis_long_txt lon,
    |a.gis_lat_txt lat,
    |regexp_replace(a.cgi,'-', '') as cgi,
    |a.cgi as orig_cgi,
    |row_number() over (partition by regexp_replace(a.cgi,'-', '') order by a.cell_id,a.base_station_id,a.base_station_nm) rnk,
    |a.tbl_dt
    |from flare_8.maps2G a
    |left outer join nigeria.dim_base_station_region b
    |on (a.base_station_id=b.site_id)
    |where a.tbl_dt={tbl_dt} and length(a.cgi)=18
    |) where rnk=1)
    |union all
    |(
    |select
    |technology,
    |cell_id,
    |base_station_id,
    |base_station_nm,
    |base_station_region_nm,
    |province_nm,
    |region_nm,
    |sub_region_nm,
    |cluster_nm,
    |site_category_nm,
    |technology_cd,
    |cluster_priority_cd,
    |site_priority_cd,
    |lon,
    |lat,
    |cgi,
    |orig_cgi,
    |tbl_dt
    |from (
    |select
    |'3G' as technology,
    |a.cell_id,
    |a.base_station_id,
    |a.base_station_nm,
    |b.base_station_region_nm base_station_region_nm,
    |a.province_nm,
    |a.region_nm,
    |a.sub_region_nm,
    |a.cluster_nm,
    |a.site_category_nm,
    |a.technology_cd,
    |a.cluster_priority_cd,
    |a.site_priority_cd,
    |a.gis_long_txt lon,
    |a.gis_lat_txt lat,
    |regexp_replace(a.cgi,'-', '') as cgi,
    |a.cgi as orig_cgi,
    |row_number() over (partition by regexp_replace(a.cgi,'-', '') order by a.cell_id,a.base_station_id,a.base_station_nm) rnk,
    |a.tbl_dt
    |from flare_8.maps3G a
    |left outer join nigeria.dim_base_station_region b
    |on (a.base_station_id=b.site_id)
    |where a.tbl_dt={tbl_dt} and length(a.cgi)=18
    |) where rnk=1)
    |union all
    |(
    |select
    |technology,
    |cell_id,
    |base_station_id,
    |base_station_nm,
    |base_station_region_nm,
    |province_nm,
    |region_nm,
    |sub_region_nm,
    |cluster_nm,
    |site_category_nm,
    |technology_cd,
    |cluster_priority_cd,
    |site_priority_cd,
    |lon,
    |lat,
    |cgi,
    |orig_cgi,
    |tbl_dt
    |from (
    |select
    |a.tbl_dt,
    |'4G' as technology,
    |a.base_station_id,
    |a.base_station_nm,
    |b.base_station_region_nm base_station_region_nm,
    |a.province_nm,
    |a.region_nm,
    |a.sub_region_nm,
    |a.cluster_nm,
    |a.site_category_nm,
    |a.technology_cd,
    |a.cluster_priority_cd,
    |a.site_priority_cd,
    |a.gis_long_txt lon,
    |a.gis_lat_txt lat,
    |regexp_replace(
    |substr(a.cgi,1,3) ||
    |substr(a.cgi,5,2) || '-' ||
    |CASE
    |WHEN length(cast(cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) ) = 8 THEN
    |cast( '00' || cast( cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) as varchar)
    |WHEN length(cast(cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) ) = 9 THEN
    |cast( '0' || cast( cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) as varchar)
    |ELSE
    |cast((cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int)) as varchar)
    |END
    |,'-','') as cgi,
    |a.cgi as orig_cgi,
    |row_number() over (partition by
    |regexp_replace(
    |substr(a.cgi,1,3) ||
    |substr(a.cgi,5,2) || '-' ||
    |CASE
    |WHEN length(cast(cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) ) = 8 THEN
    |cast( '00' || cast( cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) as varchar)
    |WHEN length(cast(cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) ) = 9 THEN
    |cast( '0' || cast( cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int) as varchar) as varchar)
    |ELSE
    |cast((cast(substr(a.cgi,8,6) as int) * 256 + cast(substr(a.cgi,-1) as int)) as varchar)
    |END
    |,'-','')
    |order by a.cell_id,a.base_station_id,a.base_station_nm) rnk,
    |a.cell_id
    |from flare_8.maps4G a
    |left outer join nigeria.dim_base_station_region b
    |on (a.base_station_id=b.site_id)
    |where a.tbl_dt={tbl_dt} and length(a.cgi)>=15
    |) where rnk=1)
    |)
  """.stripMargin
  var date=0
  def main(args: Array[String]): Unit = {
    val pool = Executors.newFixedThreadPool(4)
    Log.logMsg("start")
    parseArgs(args)
    //    val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive/flare_8", "test", null)
    Seq("hive", "-e",  "\"drop table if exists flare_8.mapsinv_cell_tmp\"") !;
            val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive5/flare_8", "test", null)

                  jDBCManager.executeQuery(Query.replace("""{tbl_dt}""",date.toString))


                  Log.logMsg("Running last query: ")
                  Seq ("hive","-e","\"set hive.exec.dynamic.partition.mode=nonstrict;insert overwrite table flare_8.mapsinv_cell partition(tbl_dt) select * from flare_8.mapsinv_cell_tmp;") !;

    Log.logMsg("done")

  }
  def parseArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--date", argDataDate: String) => date = argDataDate.toInt
      }
    else
      Log.logError("No date is supplied", true)
  }
}


// /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar



class JDBCManager(private val url: String, private val user: String, private val pass: String) {
  private val conn: Connection = DriverManager.getConnection(this.url, this.user, this.pass)

  private val maxRetry = 5

  def executeQuery(query: String) :String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    Log.logMsg(query)
    var result=1
    var status="Start"
    val table_name="flare_8.mapsinv_cell_tmp"
    Log.logMsg("->->-> " +table_name)
    while ( retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("dropping table: " +table_name)
          //Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
          Log.logMsg("generating table statement for: "+table_name)
          statement = conn.createStatement()
          Log.logMsg("executing statementfor :"+ table_name)
          result = statement.executeUpdate(query)
          Log.logMsg("successfully populated"+table_name)
          return "success"
        }
      } catch {
        case ex: Throwable => {
          Log.logMsg("reprocessing "+table_name)
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          Log.logMsg("----------->"+ table_name+": "+ex.getMessage)
          //          Log.logMsg(ex.getStackTrace.toList.foreach(println))
          ex.printStackTrace
          //Seq("hive", "-e",  "\"drop table if exists "+table_name+"\"") !;
        }
      }
    }
    //(resultSet, statement)
    status="failed"
    Log.logMsg("************************** Failed processing for "+table_name+"*****************************************")
    status
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
MapsCell.main(args)

