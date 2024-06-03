package com.ligadata.dataobject

import com.ligadata.parsers.Constants

class NinEnrollReconFeed extends Feed {

  var date: String = ""
  var totalEnrolments: Int = _
  var totalEnrolmentsWithNinToday: Int = _
  var totalEnrolmentsWithoutNinToday: Int = _
  var overallNinGenerationToday: Int = _
  var timeOfRequest: String = ""

  override def getName: String = {
    Constants.NIN_ENROLL_RECON
  }

  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(20)
    arr(0) = date
    arr(1) = totalEnrolments.toString
    arr(2) = totalEnrolmentsWithNinToday.toString
    arr(3) = totalEnrolmentsWithoutNinToday.toString
    arr(4) = overallNinGenerationToday.toString
    arr(5) = timeOfRequest

    arr
  }
}

object NinEnrollReconCls {

  case class NinEnrollRecon(date: String,
                            totalEnrolments: Int,
                            totalEnrolmentsWithNinToday: Int,
                            totalEnrolmentsWithoutNinToday: Int,
                            overallNinGenerationToday: Int,
                            timeOfRequest: String
                           )

}