package com.ligadata.parsers


import com.ligadata.dataobject.ServiceCls
import com.ligadata.dataobject.{Feed, ServiceProviderFeed}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse
import scala.xml.{Elem, XML}


object ServiceProviderParser extends Parser {
  private implicit val jsonFormats: Formats = DefaultFormats


  override def parseJson(jsonString: String): Feed = {
    try {

      val serviceMap = parse(jsonString).extract[ServiceCls.ServiceLog]
      val ServiceProviderFeed = getServiceProviderFeed(serviceMap)

      ServiceProviderFeed

    } catch {
      case ex: Exception => throw new Exception(ex.getMessage, ex)
    }
  }

  def parseXml(xmlString: String): Elem = {
    try {

      val serviceMapXml = XML.loadString(xmlString)
      serviceMapXml

    } catch {
      case ex: Exception => throw new Exception(ex.getMessage, ex)
    }
  }

  private def getServiceProviderFeed(serviceMap: ServiceCls.ServiceLog): ServiceProviderFeed = {
    val serviceProvider = new ServiceProviderFeed

    val serviceProviderXml = if (serviceMap.Message.nonEmpty && serviceMap.Message!= null ) parseXml(serviceMap.Message) else null

    val checker = if(serviceMap.Message.contains("paymentcompletedrequest")) "1" else if (serviceMap.Message.contains("paymentresponse")) "2" else if (serviceMap.Message.contains("paymentrequest")) "3" else "-1"

    val checkLength =  if(serviceProviderXml != null && (checker == "1"  || checker == "2" || checker == "3") )   serviceProviderXml.child.length else 0

    val checkXML = if(serviceProviderXml != null)  ( serviceProviderXml \ "providertransactionid").toString else ""

    serviceProvider.HTTPSTATUS = serviceMap.HttpStatus
    serviceProvider.DIRECTION = serviceMap.Direction
    serviceProvider.TRANSACTIONID = serviceMap.TransactionId
    serviceProvider.SESSIONID = serviceMap.SessionId
    serviceProvider.REQUESTID = serviceMap.RequestId
    serviceProvider.LOGDATE = serviceMap.LogDate
    serviceProvider.MESSAGES = serviceMap.Message

    if(serviceProviderXml != null) {
      if (checkLength == 5 && serviceProviderXml.child(4).child.length == 4) {

        serviceProvider.EXTENSION_INSTITUTIONNAME = (serviceProviderXml.child(4) \\ "institutionname").text
        serviceProvider.EXTENSION_AGENTMSISDN = (serviceProviderXml.child(4) \\ "agentmsisdn").text.toLong
        serviceProvider.EXTENSION_ACCOUNTNUMBER = (serviceProviderXml.child(4) \\ "accountnumber").text
        serviceProvider.EXTENSION_CUSTOMERNAME = (serviceProviderXml.child(4) \\ "customername").text

        serviceProvider.PAYMENT_TRANSACTIONID = serviceProviderXml.child(0).text
        serviceProvider.PAYMENT_PROVIDERTRANSACTIONID = serviceProviderXml.child(1).text
        serviceProvider.PAYMENT_MESSAGE = serviceProviderXml.child(2).text
        serviceProvider.PAYMENT_STATUS = serviceProviderXml.child(3).text
      }
      else if (checkLength == 2) {

        serviceProvider.PAYMENT_TRANSACTIONID = serviceProviderXml.child(0).text
        serviceProvider.PAYMENT_STATUS = serviceProviderXml.child(1).text
        if (checkXML.contains("xmlns")) {
          serviceProvider.XMLNS_NS0 = checkXML.split(" ")(1).split("=")(1).split(">")(0)
        }
      }
      else if (checkLength == 8  && serviceProviderXml.child(7).child.length == 2){

        serviceProvider.EXTENSION_INSTITUTIONNAME = (serviceProviderXml.child(7) \\ "InstitutionName").text
        serviceProvider.EXTENSION_INSTITUTIONCODE = (serviceProviderXml.child(7) \\ "InstitutionCode").text

        serviceProvider.PAYMENT_TRANSACTIONID = serviceProviderXml.child(0).text
        serviceProvider.PAYMENT_PROVIDERTRANSACTIONID = serviceProviderXml.child(1).text
        serviceProvider.PAYMENT_RECEIVINGFRI = serviceProviderXml.child(2).text
        serviceProvider.PAYMENT_MESSAGE = serviceProviderXml.child(4).text
      }
    }
    serviceProvider
  }



  def main(args: Array[String]): Unit = {

    //val x = JsonParser.parse("SERVICE","""{"HttpStatus":200,"Direction":"IN","TransactionId":722306275,"SessionId":1500389471803,"RequestId":2868567,"LogDate":"2020-12-31 23:55:00.579","Message":"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns4:paymentcompletedrequest xmlns:op=\"http://www.ericsson.com/em/emm/v1_0/common\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:ns4=\"http://www.ericsson.com/em/emm/serviceprovider/v1_1/backend\"><transactionid>92986324</transactionid><providertransactionid>YDFS92986324</providertransactionid><message>payment is pending!</message><status>PENDING</status><extension><accountnumber>0022432637</accountnumber><institutionname>union</institutionname><customername>YAHAYA USMAN</customername><agentmsisdn>2348162224999</agentmsisdn></extension></ns4:paymentcompletedrequest>"}""")
    //x.foreach(value =>{println(value)})

  }
}