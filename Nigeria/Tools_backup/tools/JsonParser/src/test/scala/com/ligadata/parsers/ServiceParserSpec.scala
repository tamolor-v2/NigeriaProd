package com.ligadata.parsers

import com.ligadata.dataobject.ServiceProviderFeed
import org.scalatest.FlatSpec

class ServiceParserSpec extends FlatSpec {

  "parseJson" should "parse valid FTLR json message " in {

    val msg = """{"HttpStatus":200,"Direction":"OUT","TransactionId":205838614,"SessionId":1500088919925,"RequestId":194308,"LogDate":"2020-08-12 16:22:20.463","Message":"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns4:paymentrequest xmlns:op=\"http://www.ericsson.com/em/emm/v1_0/common\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:ns4=\"http://www.ericsson.com/em/emm/serviceprovider/v1_3/backend/client\"><transactionid>45513683</transactionid><accountholderid>ID:2347025821878/MSISDN</accountholderid><receivingfri>FRI:2349067500257@airuser.sp/SP</receivingfri><amount><amount>100.00</amount><currency>NGN</currency></amount><transmissioncounter>1</transmissioncounter><transactiontimestamp>2020-08-12T16:22:20+0100</transactiontimestamp><extension/></ns4:paymentrequest>"}"""
    val output: Array[String] = ServiceProviderParser.parseJson(msg).getFields
    val serviceLog: ServiceProviderFeed = new ServiceProviderFeed
    serviceLog.HTTPSTATUS = 200
    serviceLog.DIRECTION = "OUT"
    serviceLog.TRANSACTIONID = 205838614L
    serviceLog.SESSIONID = 1500088919925L
    serviceLog.REQUESTID = 194308L
    serviceLog.LOGDATE = "2020-08-12 16:22:20.463"
    serviceLog.MESSAGE = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns4:paymentrequest xmlns:op=\"http://www.ericsson.com/em/emm/v1_0/common\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:ns4=\"http://www.ericsson.com/em/emm/serviceprovider/v1_3/backend/client\"><transactionid>45513683</transactionid><accountholderid>ID:2347025821878/MSISDN</accountholderid><receivingfri>FRI:2349067500257@airuser.sp/SP</receivingfri><amount><amount>100.00</amount><currency>NGN</currency></amount><transmissioncounter>1</transmissioncounter><transactiontimestamp>2020-08-12T16:22:20+0100</transactiontimestamp><extension/></ns4:paymentrequest>"
    val expected: Array[String] = serviceLog.getFields
    println(serviceLog.getFields.mkString(","))
    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println(output.mkString(","))
    assert(output.sameElements(expected)
    )
  }

}