package com.ligadata.dataobject

import com.ligadata.parsers.Constants

class NinEnrollFeed extends Feed {

  var trackingId: String = ""
  var enrollmentTime: String = ""
  var timeSynced: String = ""
  var ninGenerationDate: String = ""
  var clientEnrolmentType: String = ""
  var loginId: String = ""
  var enrollerFirstName: String = ""
  var enrollerLastName: String = ""
  var enrollerPhoneNumber: String = ""
  var enrollerEmail: String = ""
  var ninStatus: String = ""
  var lgaCode: String = ""
  var lgaName: String = ""
  var currentLocationLatitude: Double = _
  var currentLocationLongitude: Double = _
  var nodeId: String = ""
  var nodeMachineTag: String = ""
  var originatingCenterCode: String = ""
  var originatingCenterName: String = ""
  var deviceType: String = ""

  override def getName: String = {
    Constants.NIN_ENROLL
  }

  override def getFields: Array[String] = {
    val arr: Array[String] = new Array(20)
    arr(0) = trackingId
    arr(1) = enrollmentTime
    arr(2) = timeSynced
    arr(3) = ninGenerationDate
    arr(4) = clientEnrolmentType
    arr(5) = loginId
    arr(6) = enrollerFirstName
    arr(7) = enrollerLastName
    arr(8) = enrollerPhoneNumber
    arr(9) = enrollerEmail
    arr(10) = ninStatus
    arr(11) = lgaCode
    arr(12) = lgaName
    arr(13) = currentLocationLatitude.toString
    arr(14) = currentLocationLongitude.toString
    arr(15) = nodeId
    arr(16) = nodeMachineTag
    arr(17) = originatingCenterCode
    arr(18) = originatingCenterName
    arr(19) = deviceType

    arr
  }
}

object NinEnrollCls {

  case class NinEnroll(trackingId: String,
                       enrollmentTime: String,
                       timeSynced: String,
                       ninGenerationDate: String,
                       clientEnrolmentType: String,
                       loginId: String,
                       enrollerFirstName: String,
                       enrollerLastName: String,
                       enrollerPhoneNumber: String,
                       enrollerEmail: String,
                       ninStatus: String,
                       lgaCode: String,
                       lgaName: String,
                       currentLocationLatitude: Double,
                       currentLocationLongitude: Double,
                       nodeId: String,
                       nodeMachineTag: String,
                       originatingCenterCode: String,
                       originatingCenterName: String,
                       deviceType: String
                      )

}