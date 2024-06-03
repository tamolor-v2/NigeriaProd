package com.ligadata.parsers

object JsonParser {

  def parse(name: String, jsonString: String): Array[String] = {
    var arr: Array[String] = Array()
    if (name != null) {
      name.toUpperCase match {
        case Constants.FIN_LOG =>
          arr = FinLogParser.parseJson(jsonString).getFields
        case Constants.SESSION =>
          arr = SessionParser.parseJson(jsonString).getFields
        case Constants.PSB_AUDIT =>
          arr = PSBAuditParser.parseJson(jsonString).getFields
        case Constants.AUDIT =>
          arr = AuditParser.parseJson(jsonString).getFields
        case Constants.SERVICE =>
          arr = ServiceProviderParser.parseJson(jsonString).getFields
        case Constants.PROVISIONING =>
          arr = ProvisioningParser.parseJson(jsonString).getFields
        case Constants.NIN_ENROLL =>
          arr = NinEnrollParser.parseJson(jsonString).getFields
        case Constants.NIN_ENROLL_RECON =>
          arr = NinEnrollReconParser.parseJson(jsonString).getFields
        case _ => throw new Exception(s"Type:$name not supported yet")
      }
    }
    arr
  }
}

object Constants {
  val DELIMITER: String = "\b"
  val FIN_LOG = "FIN_LOG"
  val SESSION = "SESSION"
  val PSB_AUDIT = "PSB_AUDIT"
  val AUDIT = "AUDIT"
  val PROVISIONING = "PROVISIONING"
  val SERVICE = "SERVICE"
  val NIN_ENROLL = "NIN_ENROLL"
  val NIN_ENROLL_RECON = "NIN_ENROLL_RECON"
}

object ConversionHelper {
  def getDefaultString(vl: Option[String]): String = {
    if (vl.isDefined) vl.get.toString else ""
  }

  def getDefaultDouble(vl: Option[Double]): Double = {
    if (vl.isDefined) vl.get else 0.0
  }

  def getDefaultInt(vl: Option[Int]): Int = {
    if (vl.isDefined) vl.get else 0
  }

  def getDefaultLong(vl: Option[Long]): Long = {
    if (vl.isDefined) vl.get else 0L
  }

}