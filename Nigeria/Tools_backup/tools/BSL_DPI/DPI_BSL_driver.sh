#!/bin/sh
exec scala -cp /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar -savecompiled "$0" "$@"
!#

import java.io._
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.Duration

import collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source.fromFile
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.sql.{Connection, DriverManager, ResultSet, Statement}


object DPI_BSL {

def main(args: Array[String]): Unit = {

println("start")
val jDBCManager = new JDBCManager("jdbc:presto://master01004.mtn.com:8099/hive/flare_8", "test", null)

val str ="select * from flare_8.cs5_ccn_voice_ma limit 10 "
jDBCManager.executeQuery(str)

println("done")

}


}


// /mnt/beegfs/Deployment/DEV/scripts/BslScript/presto-jdbc-0.191.jar



class JDBCManager(private val url: String, private val user: String, private val pass: String) {
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
          println("Exception while executing query: %s ".format(query))
          println("Retry: %2d/%2d".format(retry, maxRetry))
          ex.printStackTrace()
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



DPI_BSL.main(args)
 
