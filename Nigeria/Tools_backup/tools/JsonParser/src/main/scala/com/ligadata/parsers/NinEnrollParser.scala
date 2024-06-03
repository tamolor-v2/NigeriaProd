package com.ligadata.parsers

import com.ligadata.dataobject.NinEnrollCls
import com.ligadata.dataobject.{NinEnrollFeed, Feed}
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.jackson.JsonMethods.parse

object NinEnrollParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats


  override def parseJson(jsonString: String): Feed = {
    try {
      val ninEnrollMap = parse(jsonString).extract[NinEnrollCls.NinEnroll]
      val NinEnrollFeed = getNinEnrollFeed(ninEnrollMap)

      NinEnrollFeed
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage, ex)
      }
    }
  }

  private def getNinEnrollFeed(ninEnrollMap: NinEnrollCls.NinEnroll): NinEnrollFeed = {
    val ninEnroll = new NinEnrollFeed

    ninEnroll.trackingId = ninEnrollMap.trackingId
    ninEnroll.enrollmentTime = ninEnrollMap.enrollmentTime
    ninEnroll.timeSynced = ninEnrollMap.timeSynced
    ninEnroll.ninGenerationDate = ninEnrollMap.ninGenerationDate
    ninEnroll.clientEnrolmentType = ninEnrollMap.clientEnrolmentType
    ninEnroll.loginId = ninEnrollMap.loginId
    ninEnroll.enrollerFirstName = ninEnrollMap.enrollerFirstName
    ninEnroll.enrollerLastName = ninEnrollMap.enrollerLastName
    ninEnroll.enrollerPhoneNumber = ninEnrollMap.enrollerPhoneNumber
    ninEnroll.enrollerEmail = ninEnrollMap.enrollerEmail
    ninEnroll.ninStatus = ninEnrollMap.ninStatus
    ninEnroll.lgaCode = ninEnrollMap.lgaCode
    ninEnroll.lgaName = ninEnrollMap.lgaName
    ninEnroll.currentLocationLatitude = ninEnrollMap.currentLocationLatitude
    ninEnroll.currentLocationLongitude = ninEnrollMap.currentLocationLongitude
    ninEnroll.nodeId = ninEnrollMap.nodeId
    ninEnroll.nodeMachineTag = ninEnrollMap.nodeMachineTag
    ninEnroll.originatingCenterCode = ninEnrollMap.originatingCenterCode
    ninEnroll.originatingCenterName = ninEnrollMap.originatingCenterName
    ninEnroll.deviceType = ninEnrollMap.deviceType
    ninEnroll
  }
}