package com.ligadata.parsers

import com.ligadata.dataobject.NinEnrollReconCls
import com.ligadata.dataobject.{NinEnrollReconFeed, Feed}
import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.jackson.JsonMethods.parse

object NinEnrollReconParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats


  override def parseJson(jsonString: String): Feed = {
    try {
      val ninEnrollReconMap = parse(jsonString).extract[NinEnrollReconCls.NinEnrollRecon]
      val NinEnrollReconFeed = getNinEnrollReconFeed(ninEnrollReconMap)

      NinEnrollReconFeed
    } catch {
      case ex: Exception => {
        throw new Exception(ex.getMessage, ex)
      }
    }
  }

  private def getNinEnrollReconFeed(ninEnrollReconMap: NinEnrollReconCls.NinEnrollRecon): NinEnrollReconFeed = {
    val ninEnrollRecon = new NinEnrollReconFeed

    ninEnrollRecon.date = ninEnrollReconMap.date
    ninEnrollRecon.totalEnrolments = ninEnrollReconMap.totalEnrolments
    ninEnrollRecon.totalEnrolmentsWithNinToday = ninEnrollReconMap.totalEnrolmentsWithNinToday
    ninEnrollRecon.totalEnrolmentsWithoutNinToday = ninEnrollReconMap.totalEnrolmentsWithoutNinToday
    ninEnrollRecon.overallNinGenerationToday = ninEnrollReconMap.overallNinGenerationToday
    ninEnrollRecon.timeOfRequest = ninEnrollReconMap.timeOfRequest
    ninEnrollRecon
  }
}