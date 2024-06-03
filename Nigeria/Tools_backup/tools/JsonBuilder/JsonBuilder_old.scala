#!/bin/sh
exec scala -cp /mnt/beegfs/tools/JsonBuilder/ExtDependencyLibs_2.11-1.5.3.jar -savecompiled "$0" "$@"
!#


import java.text.SimpleDateFormat
import java.util.Date

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
/**
  * Created by Yousef on 10/27/2018.
  */
object JsonBuilder {

  case class JobInfo(id: Int, runTime: String, status: Int, logDirectory: String, hostName: String, errorMessage: String, errorMessageDescription: String, step: Int, generalMessage: String, runID: Int)

  var id: Int  = -1
  var run_time: String = ""
  var status: Int = -2
  var log_directory: String = ""
  var hostname: String = ""
  var error_message: String = ""
  var error_message_description: String = ""
  var step: Int = 1
  var general_message = ""
  var helpString = ""
  val runTimeDateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val runIdDateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

  def getDateTime(option: String): String ={
    val submittedDateConvert = new Date()
    val submittedDateTime =
      if(option.equals("runTime"))
        runTimeDateFormatter.format(submittedDateConvert)
      else
        runIdDateFormatter.format(submittedDateConvert)
    submittedDateTime
  }

  def parsArgs = (args: Array[String]) => {
    args.sliding(2, 2).toList.collect {
      case Array("--id", argId: String) => id = argId.toInt
      case Array("--status", argStatus: String) => status = argStatus.toInt
      case Array("--log_directory", argLogDirectory: String) => log_directory = argLogDirectory
      case Array("--hostname", argHostName: String) => hostname = argHostName
      case Array("--error_message", argErrorMessage: String) =>  error_message = argErrorMessage.trim.toLowerCase
      case Array("--error_message_description", argErrorMessageDescription) =>  error_message_description = argErrorMessageDescription
      case Array("--step", argStep) =>  step = argStep.toInt
      case Array("--general_message", argGeneralMessage: String) =>  general_message = argGeneralMessage
      case Array("--help") | _ => helpString = "--id  --status --log_directory  --hostname --error_message --error_message_description --step --general_message --help"
        System.exit(0)
    }
  }

  def validateArgs(): Unit ={
    if (id == -1 ) {
      println("Error: id = -1")
      System.exit(1)
    }

    if (status == -2) {
      println("Error: status = -2")
      System.exit(1)
    }

    if (hostname == null || hostname.length == 0) {
      println("Error: hostname is null")
      System.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val formats = DefaultFormats
    parsArgs(args)
    validateArgs
    val jobDesc = JobInfo(id, getDateTime("runTime"), status, log_directory, hostname, error_message, error_message_description, step, general_message, getDateTime("runID").toInt)
    println(write(jobDesc))
  }
}

