package com.mtn.messages;
import org.json4s.jackson.JsonMethods._;
import org.json4s.DefaultFormats;
import org.json4s.Formats;
import com.ligadata.KamanjaBase._;
import com.ligadata.BaseTypes._;
import com.ligadata.Exceptions._;
import org.apache.logging.log4j.{ Logger, LogManager }
import java.util.Date;
import java.io.{ DataInputStream, DataOutputStream, ByteArrayOutputStream }
import java.nio.ByteBuffer
    
 
object CS5_CCN_GPRS_MA extends RDDObject[CS5_CCN_GPRS_MA] with MessageFactoryInterface { 
 
  val log = LogManager.getLogger(getClass)
	type T = CS5_CCN_GPRS_MA ;
	override def getFullTypeName: String = "com.mtn.messages.CS5_CCN_GPRS_MA"; 
	override def getTypeNameSpace: String = "com.mtn.messages"; 
	override def getTypeName: String = "CS5_CCN_GPRS_MA"; 
	override def getTypeVersion: String = "000000.000001.000000"; 
	override def getSchemaId: Int = 2001604; 
	private var elementId: Long = 0L; 
	override def setElementId(elemId: Long): Unit = { elementId = elemId; } 
	override def getElementId: Long = elementId; 
	override def getTenantId: String = "flare"; 
	override def createInstance: CS5_CCN_GPRS_MA = new CS5_CCN_GPRS_MA(CS5_CCN_GPRS_MA); 
	override def isFixed: Boolean = true; 
	def isCaseSensitive(): Boolean = false; 
	override def getContainerType: ContainerTypes.ContainerType = ContainerTypes.ContainerType.MESSAGE
	override def getFullName = getFullTypeName; 
	override def getRddTenantId = getTenantId; 
	override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this); 

    def build = new T(this)
    def build(from: T) = new T(from)
   override def getPartitionKeyNames: Array[String] = Array("msisdn_key"); 

  override def getPrimaryKeyNames: Array[String] = Array[String](); 
   
  
  override def getTimePartitionInfo: TimePartitionInfo = {
    var timePartitionInfo: TimePartitionInfo = new TimePartitionInfo();
    timePartitionInfo.setFieldName("date_key");
    timePartitionInfo.setFormat("yyyyMMdd");
    timePartitionInfo.setTimePartitionType(TimePartitionInfo.TimePartitionType.DAILY);
    return timePartitionInfo
  }

       
    override def hasPrimaryKey(): Boolean = {
      val pKeys = getPrimaryKeyNames();
      return (pKeys != null && pKeys.length > 0);
    }

    override def hasPartitionKey(): Boolean = {
      val pKeys = getPartitionKeyNames();
      return (pKeys != null && pKeys.length > 0);
    }

    override def hasTimePartitionInfo(): Boolean = {
      val tmInfo = getTimePartitionInfo();
      return (tmInfo != null && tmInfo.getTimePartitionType != TimePartitionInfo.TimePartitionType.NONE);
    }
  
    override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.messages" , "name" : "cs5_ccn_gprs_ma" , "fields":[{ "name" : "switchcalltype" , "type" : "string"},{ "name" : "chrononumber" , "type" : "string"},{ "name" : "imsinumber" , "type" : "string"},{ "name" : "imeinumber" , "type" : "string"},{ "name" : "mobilenumber" , "type" : "string"},{ "name" : "calldate" , "type" : "long"},{ "name" : "chargingid" , "type" : "string"},{ "name" : "callduration" , "type" : "string"},{ "name" : "accesspointname" , "type" : "string"},{ "name" : "pdptype" , "type" : "string"},{ "name" : "pdpaddress" , "type" : "string"},{ "name" : "cellid" , "type" : "string"},{ "name" : "partialtypeindicator" , "type" : "string"},{ "name" : "callterminationcause" , "type" : "string"},{ "name" : "chargingcharacteristics" , "type" : "string"},{ "name" : "sgsnaddress" , "type" : "string"},{ "name" : "ggsnaddress" , "type" : "string"},{ "name" : "qosrequested1" , "type" : "string"},{ "name" : "qosused1" , "type" : "string"},{ "name" : "datavolumeoutgoing1" , "type" : "string"},{ "name" : "datavolumeincoming1" , "type" : "string"},{ "name" : "qosrequested2" , "type" : "string"},{ "name" : "qosused2" , "type" : "string"},{ "name" : "datavolumeoutgoing2" , "type" : "string"},{ "name" : "datavolumeincoming2" , "type" : "string"},{ "name" : "qosrequested3" , "type" : "string"},{ "name" : "qosused3" , "type" : "string"},{ "name" : "datavolumeoutgoing3" , "type" : "string"},{ "name" : "datavolumeincoming3" , "type" : "string"},{ "name" : "recordtype" , "type" : "string"},{ "name" : "amountofsdpbeforesession" , "type" : "string"},{ "name" : "amountonsdpaftersession" , "type" : "string"},{ "name" : "costofsession" , "type" : "string"},{ "name" : "dedicatedaccountvaluebeforecall" , "type" : "string"},{ "name" : "dedicatedaccountvalueaftercall" , "type" : "string"},{ "name" : "dedicatedaccountid" , "type" : "string"},{ "name" : "dedicatedamountused" , "type" : "string"},{ "name" : "numberofdedicatedaccountused" , "type" : "string"},{ "name" : "friendsandfamilyindicator" , "type" : "string"},{ "name" : "friendsandfamilynumber" , "type" : "string"},{ "name" : "ratedunitsfreeunits" , "type" : "string"},{ "name" : "ratedunitsdebit" , "type" : "string"},{ "name" : "ratedunitscredit" , "type" : "string"},{ "name" : "offerid" , "type" : "string"},{ "name" : "offertype" , "type" : "string"},{ "name" : "bonusamount" , "type" : "string"},{ "name" : "accountgroupid" , "type" : "string"},{ "name" : "pamserviceid" , "type" : "string"},{ "name" : "pamclassid" , "type" : "string"},{ "name" : "selectiontreetype" , "type" : "string"},{ "name" : "servedaccount" , "type" : "string"},{ "name" : "servedofferings" , "type" : "string"},{ "name" : "terminationcause" , "type" : "string"},{ "name" : "chargingcontextid" , "type" : "string"},{ "name" : "servicecontextid" , "type" : "string"},{ "name" : "servicesessionidcallreferencenumber" , "type" : "string"},{ "name" : "sourcefilename" , "type" : "string"},{ "name" : "networkcd" , "type" : "string"},{ "name" : "serviceclassid" , "type" : "string"},{ "name" : "paytype" , "type" : "string"},{ "name" : "callreferenceid" , "type" : "string"},{ "name" : "mainamountused" , "type" : "string"},{ "name" : "serviceprovider_identifier" , "type" : "string"},{ "name" : "vascode" , "type" : "string"},{ "name" : "originatingservices" , "type" : "string"},{ "name" : "originatingstation" , "type" : "string"},{ "name" : "terminatingstation" , "type" : "string"},{ "name" : "subscriptiontype" , "type" : "string"},{ "name" : "transid" , "type" : "string"},{ "name" : "channel" , "type" : "string"},{ "name" : "calledcallingnumber" , "type" : "string"},{ "name" : "eventtime" , "type" : "string"},{ "name" : "category" , "type" : "string"},{ "name" : "sucategory" , "type" : "string"},{ "name" : "content_name" , "type" : "string"},{ "name" : "terminating_number" , "type" : "string"},{ "name" : "compositerecords" , "type" : "string"},{ "name" : "file_name" , "type" : "string"},{ "name" : "file_offset" , "type" : "long"},{ "name" : "kamanja_loaded_date" , "type" : "string"},{ "name" : "file_mod_date" , "type" : "string"},{ "name" : "bill_type_enrich" , "type" : "int"},{ "name" : "network_type_enrich" , "type" : "int"},{ "name" : "data_type_enrich" , "type" : "int"},{ "name" : "tac_enrich" , "type" : "string"},{ "name" : "normalizedchargedpartycgi_enrich" , "type" : "string"},{ "name" : "latitude_enrich" , "type" : "double"},{ "name" : "longitude_enrich" , "type" : "double"},{ "name" : "locationtype_enrich" , "type" : "int"},{ "name" : "date_key" , "type" : "int"},{ "name" : "msisdn_key" , "type" : "long"},{ "name" : "event_timestamp_enrich" , "type" : "long"},{ "name" : "original_timestamp_enrich" , "type" : "string"},{ "name" : "is_free_enrich" , "type" : "int"},{ "name" : "data_volume_incoming_1" , "type" : "long"},{ "name" : "data_volume_outgoing_1" , "type" : "long"},{ "name" : "ma_amount_used" , "type" : "double"},{ "name" : "imei_number" , "type" : "string"},{ "name" : "imsi_number" , "type" : "string"},{ "name" : "msg_unique_id_enrich" , "type" : "long"},{ "name" : "base_file_name" , "type" : "string"},{ "name" : "path" , "type" : "string"},{ "name" : "line_number" , "type" : "long"},{ "name" : "file_id" , "type" : "string"},{ "name" : "processed_timestamp" , "type" : "long"},{ "name" : "ltz_event_timestamp_enrich" , "type" : "long"},{ "name" : "transactionid_pp3" , "type" : "string"},{ "name" : "chargeamount_pp3" , "type" : "double"},{ "name" : "kamanja_system_null_flags" , "type" :  {"type" : "array", "items" : "boolean"}}]}""";  
  
      var attributeTypes = generateAttributeTypes;
    
    private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
      var attributeTypes = new Array[AttributeTypeInfo](109);
                 attributeTypes(0) = new AttributeTypeInfo("switchcalltype", 0, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(1) = new AttributeTypeInfo("chrononumber", 1, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(2) = new AttributeTypeInfo("imsinumber", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(3) = new AttributeTypeInfo("imeinumber", 3, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(4) = new AttributeTypeInfo("mobilenumber", 4, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(5) = new AttributeTypeInfo("calldate", 5, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(6) = new AttributeTypeInfo("chargingid", 6, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(7) = new AttributeTypeInfo("callduration", 7, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(8) = new AttributeTypeInfo("accesspointname", 8, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(9) = new AttributeTypeInfo("pdptype", 9, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(10) = new AttributeTypeInfo("pdpaddress", 10, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(11) = new AttributeTypeInfo("cellid", 11, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(12) = new AttributeTypeInfo("partialtypeindicator", 12, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(13) = new AttributeTypeInfo("callterminationcause", 13, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(14) = new AttributeTypeInfo("chargingcharacteristics", 14, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(15) = new AttributeTypeInfo("sgsnaddress", 15, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(16) = new AttributeTypeInfo("ggsnaddress", 16, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(17) = new AttributeTypeInfo("qosrequested1", 17, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(18) = new AttributeTypeInfo("qosused1", 18, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(19) = new AttributeTypeInfo("datavolumeoutgoing1", 19, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(20) = new AttributeTypeInfo("datavolumeincoming1", 20, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(21) = new AttributeTypeInfo("qosrequested2", 21, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(22) = new AttributeTypeInfo("qosused2", 22, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(23) = new AttributeTypeInfo("datavolumeoutgoing2", 23, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(24) = new AttributeTypeInfo("datavolumeincoming2", 24, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(25) = new AttributeTypeInfo("qosrequested3", 25, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(26) = new AttributeTypeInfo("qosused3", 26, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(27) = new AttributeTypeInfo("datavolumeoutgoing3", 27, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(28) = new AttributeTypeInfo("datavolumeincoming3", 28, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(29) = new AttributeTypeInfo("recordtype", 29, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(30) = new AttributeTypeInfo("amountofsdpbeforesession", 30, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(31) = new AttributeTypeInfo("amountonsdpaftersession", 31, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(32) = new AttributeTypeInfo("costofsession", 32, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(33) = new AttributeTypeInfo("dedicatedaccountvaluebeforecall", 33, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(34) = new AttributeTypeInfo("dedicatedaccountvalueaftercall", 34, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(35) = new AttributeTypeInfo("dedicatedaccountid", 35, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(36) = new AttributeTypeInfo("dedicatedamountused", 36, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(37) = new AttributeTypeInfo("numberofdedicatedaccountused", 37, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(38) = new AttributeTypeInfo("friendsandfamilyindicator", 38, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(39) = new AttributeTypeInfo("friendsandfamilynumber", 39, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(40) = new AttributeTypeInfo("ratedunitsfreeunits", 40, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(41) = new AttributeTypeInfo("ratedunitsdebit", 41, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(42) = new AttributeTypeInfo("ratedunitscredit", 42, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(43) = new AttributeTypeInfo("offerid", 43, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(44) = new AttributeTypeInfo("offertype", 44, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(45) = new AttributeTypeInfo("bonusamount", 45, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(46) = new AttributeTypeInfo("accountgroupid", 46, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(47) = new AttributeTypeInfo("pamserviceid", 47, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(48) = new AttributeTypeInfo("pamclassid", 48, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(49) = new AttributeTypeInfo("selectiontreetype", 49, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(50) = new AttributeTypeInfo("servedaccount", 50, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(51) = new AttributeTypeInfo("servedofferings", 51, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(52) = new AttributeTypeInfo("terminationcause", 52, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(53) = new AttributeTypeInfo("chargingcontextid", 53, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(54) = new AttributeTypeInfo("servicecontextid", 54, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(55) = new AttributeTypeInfo("servicesessionidcallreferencenumber", 55, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(56) = new AttributeTypeInfo("sourcefilename", 56, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(57) = new AttributeTypeInfo("networkcd", 57, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(58) = new AttributeTypeInfo("serviceclassid", 58, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(59) = new AttributeTypeInfo("paytype", 59, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(60) = new AttributeTypeInfo("callreferenceid", 60, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(61) = new AttributeTypeInfo("mainamountused", 61, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(62) = new AttributeTypeInfo("serviceprovider_identifier", 62, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(63) = new AttributeTypeInfo("vascode", 63, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(64) = new AttributeTypeInfo("originatingservices", 64, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(65) = new AttributeTypeInfo("originatingstation", 65, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(66) = new AttributeTypeInfo("terminatingstation", 66, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(67) = new AttributeTypeInfo("subscriptiontype", 67, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(68) = new AttributeTypeInfo("transid", 68, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(69) = new AttributeTypeInfo("channel", 69, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(70) = new AttributeTypeInfo("calledcallingnumber", 70, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(71) = new AttributeTypeInfo("eventtime", 71, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(72) = new AttributeTypeInfo("category", 72, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(73) = new AttributeTypeInfo("sucategory", 73, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(74) = new AttributeTypeInfo("content_name", 74, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(75) = new AttributeTypeInfo("terminating_number", 75, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(76) = new AttributeTypeInfo("compositerecords", 76, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(77) = new AttributeTypeInfo("file_name", 77, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(78) = new AttributeTypeInfo("file_offset", 78, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(79) = new AttributeTypeInfo("kamanja_loaded_date", 79, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(80) = new AttributeTypeInfo("file_mod_date", 80, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(81) = new AttributeTypeInfo("bill_type_enrich", 81, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(82) = new AttributeTypeInfo("network_type_enrich", 82, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(83) = new AttributeTypeInfo("data_type_enrich", 83, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(84) = new AttributeTypeInfo("tac_enrich", 84, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(85) = new AttributeTypeInfo("normalizedchargedpartycgi_enrich", 85, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(86) = new AttributeTypeInfo("latitude_enrich", 86, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(87) = new AttributeTypeInfo("longitude_enrich", 87, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(88) = new AttributeTypeInfo("locationtype_enrich", 88, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(89) = new AttributeTypeInfo("date_key", 89, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(90) = new AttributeTypeInfo("msisdn_key", 90, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(91) = new AttributeTypeInfo("event_timestamp_enrich", 91, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(92) = new AttributeTypeInfo("original_timestamp_enrich", 92, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(93) = new AttributeTypeInfo("is_free_enrich", 93, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(94) = new AttributeTypeInfo("data_volume_incoming_1", 94, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(95) = new AttributeTypeInfo("data_volume_outgoing_1", 95, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(96) = new AttributeTypeInfo("ma_amount_used", 96, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(97) = new AttributeTypeInfo("imei_number", 97, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(98) = new AttributeTypeInfo("imsi_number", 98, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(99) = new AttributeTypeInfo("msg_unique_id_enrich", 99, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(100) = new AttributeTypeInfo("base_file_name", 100, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(101) = new AttributeTypeInfo("path", 101, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(102) = new AttributeTypeInfo("line_number", 102, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(103) = new AttributeTypeInfo("file_id", 103, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(104) = new AttributeTypeInfo("processed_timestamp", 104, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(105) = new AttributeTypeInfo("ltz_event_timestamp_enrich", 105, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(106) = new AttributeTypeInfo("transactionid_pp3", 106, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(107) = new AttributeTypeInfo("chargeamount_pp3", 107, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(108) = new AttributeTypeInfo("kamanja_system_null_flags", 108, AttributeTypeInfo.TypeCategory.ARRAY, 7, -1, 0)


      return attributeTypes
    }
    

		 var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
				def setFn0(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.switchcalltype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field switchcalltype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn1(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.chrononumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field chrononumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn2(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.imsinumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field imsinumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn3(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.imeinumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field imeinumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn4(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.mobilenumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field mobilenumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn5(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.calldate = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field calldate in message CS5_CCN_GPRS_MA") 
				} 
				def setFn6(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.chargingid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field chargingid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn7(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.callduration = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field callduration in message CS5_CCN_GPRS_MA") 
				} 
				def setFn8(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accesspointname = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accesspointname in message CS5_CCN_GPRS_MA") 
				} 
				def setFn9(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.pdptype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field pdptype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn10(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.pdpaddress = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field pdpaddress in message CS5_CCN_GPRS_MA") 
				} 
				def setFn11(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.cellid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field cellid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn12(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.partialtypeindicator = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field partialtypeindicator in message CS5_CCN_GPRS_MA") 
				} 
				def setFn13(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.callterminationcause = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field callterminationcause in message CS5_CCN_GPRS_MA") 
				} 
				def setFn14(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.chargingcharacteristics = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field chargingcharacteristics in message CS5_CCN_GPRS_MA") 
				} 
				def setFn15(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.sgsnaddress = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field sgsnaddress in message CS5_CCN_GPRS_MA") 
				} 
				def setFn16(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.ggsnaddress = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field ggsnaddress in message CS5_CCN_GPRS_MA") 
				} 
				def setFn17(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.qosrequested1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field qosrequested1 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn18(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.qosused1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field qosused1 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn19(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.datavolumeoutgoing1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field datavolumeoutgoing1 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn20(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.datavolumeincoming1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field datavolumeincoming1 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn21(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.qosrequested2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field qosrequested2 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn22(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.qosused2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field qosused2 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn23(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.datavolumeoutgoing2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field datavolumeoutgoing2 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn24(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.datavolumeincoming2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field datavolumeincoming2 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn25(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.qosrequested3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field qosrequested3 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn26(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.qosused3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field qosused3 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn27(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.datavolumeoutgoing3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field datavolumeoutgoing3 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn28(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.datavolumeincoming3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field datavolumeincoming3 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn29(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.recordtype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field recordtype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn30(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.amountofsdpbeforesession = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field amountofsdpbeforesession in message CS5_CCN_GPRS_MA") 
				} 
				def setFn31(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.amountonsdpaftersession = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field amountonsdpaftersession in message CS5_CCN_GPRS_MA") 
				} 
				def setFn32(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.costofsession = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field costofsession in message CS5_CCN_GPRS_MA") 
				} 
				def setFn33(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.dedicatedaccountvaluebeforecall = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field dedicatedaccountvaluebeforecall in message CS5_CCN_GPRS_MA") 
				} 
				def setFn34(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.dedicatedaccountvalueaftercall = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field dedicatedaccountvalueaftercall in message CS5_CCN_GPRS_MA") 
				} 
				def setFn35(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.dedicatedaccountid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field dedicatedaccountid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn36(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.dedicatedamountused = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field dedicatedamountused in message CS5_CCN_GPRS_MA") 
				} 
				def setFn37(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.numberofdedicatedaccountused = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field numberofdedicatedaccountused in message CS5_CCN_GPRS_MA") 
				} 
				def setFn38(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.friendsandfamilyindicator = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field friendsandfamilyindicator in message CS5_CCN_GPRS_MA") 
				} 
				def setFn39(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.friendsandfamilynumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field friendsandfamilynumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn40(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.ratedunitsfreeunits = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field ratedunitsfreeunits in message CS5_CCN_GPRS_MA") 
				} 
				def setFn41(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.ratedunitsdebit = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field ratedunitsdebit in message CS5_CCN_GPRS_MA") 
				} 
				def setFn42(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.ratedunitscredit = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field ratedunitscredit in message CS5_CCN_GPRS_MA") 
				} 
				def setFn43(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn44(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn45(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.bonusamount = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field bonusamount in message CS5_CCN_GPRS_MA") 
				} 
				def setFn46(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountgroupid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountgroupid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn47(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.pamserviceid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field pamserviceid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn48(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.pamclassid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field pamclassid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn49(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreetype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreetype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn50(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.servedaccount = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field servedaccount in message CS5_CCN_GPRS_MA") 
				} 
				def setFn51(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.servedofferings = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field servedofferings in message CS5_CCN_GPRS_MA") 
				} 
				def setFn52(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.terminationcause = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field terminationcause in message CS5_CCN_GPRS_MA") 
				} 
				def setFn53(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.chargingcontextid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field chargingcontextid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn54(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.servicecontextid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field servicecontextid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn55(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.servicesessionidcallreferencenumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field servicesessionidcallreferencenumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn56(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.sourcefilename = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field sourcefilename in message CS5_CCN_GPRS_MA") 
				} 
				def setFn57(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.networkcd = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field networkcd in message CS5_CCN_GPRS_MA") 
				} 
				def setFn58(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.serviceclassid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field serviceclassid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn59(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.paytype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field paytype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn60(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.callreferenceid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field callreferenceid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn61(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.mainamountused = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field mainamountused in message CS5_CCN_GPRS_MA") 
				} 
				def setFn62(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.serviceprovider_identifier = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field serviceprovider_identifier in message CS5_CCN_GPRS_MA") 
				} 
				def setFn63(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.vascode = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field vascode in message CS5_CCN_GPRS_MA") 
				} 
				def setFn64(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.originatingservices = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field originatingservices in message CS5_CCN_GPRS_MA") 
				} 
				def setFn65(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.originatingstation = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field originatingstation in message CS5_CCN_GPRS_MA") 
				} 
				def setFn66(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.terminatingstation = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field terminatingstation in message CS5_CCN_GPRS_MA") 
				} 
				def setFn67(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.subscriptiontype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field subscriptiontype in message CS5_CCN_GPRS_MA") 
				} 
				def setFn68(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.transid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field transid in message CS5_CCN_GPRS_MA") 
				} 
				def setFn69(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.channel = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field channel in message CS5_CCN_GPRS_MA") 
				} 
				def setFn70(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.calledcallingnumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field calledcallingnumber in message CS5_CCN_GPRS_MA") 
				} 
				def setFn71(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.eventtime = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field eventtime in message CS5_CCN_GPRS_MA") 
				} 
				def setFn72(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.category = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field category in message CS5_CCN_GPRS_MA") 
				} 
				def setFn73(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.sucategory = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field sucategory in message CS5_CCN_GPRS_MA") 
				} 
				def setFn74(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.content_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field content_name in message CS5_CCN_GPRS_MA") 
				} 
				def setFn75(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.terminating_number = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field terminating_number in message CS5_CCN_GPRS_MA") 
				} 
				def setFn76(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.compositerecords = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field compositerecords in message CS5_CCN_GPRS_MA") 
				} 
				def setFn77(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.file_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field file_name in message CS5_CCN_GPRS_MA") 
				} 
				def setFn78(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.file_offset = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field file_offset in message CS5_CCN_GPRS_MA") 
				} 
				def setFn79(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.kamanja_loaded_date = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field kamanja_loaded_date in message CS5_CCN_GPRS_MA") 
				} 
				def setFn80(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.file_mod_date = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field file_mod_date in message CS5_CCN_GPRS_MA") 
				} 
				def setFn81(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.bill_type_enrich = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field bill_type_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn82(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.network_type_enrich = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field network_type_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn83(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.data_type_enrich = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field data_type_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn84(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.tac_enrich = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field tac_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn85(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.normalizedchargedpartycgi_enrich = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field normalizedchargedpartycgi_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn86(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.latitude_enrich = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field latitude_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn87(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.longitude_enrich = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field longitude_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn88(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.locationtype_enrich = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field locationtype_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn89(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]){ 
				  curObj.date_key = value.asInstanceOf[Int]; 
				  curObj.setTimePartitionData; 
				} else throw new Exception(s"Value is the not the correct type Int for field date_key in message CS5_CCN_GPRS_MA") 
				} 
				def setFn90(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.msisdn_key = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field msisdn_key in message CS5_CCN_GPRS_MA") 
				} 
				def setFn91(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.event_timestamp_enrich = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field event_timestamp_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn92(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.original_timestamp_enrich = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field original_timestamp_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn93(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.is_free_enrich = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field is_free_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn94(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.data_volume_incoming_1 = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field data_volume_incoming_1 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn95(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.data_volume_outgoing_1 = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field data_volume_outgoing_1 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn96(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.ma_amount_used = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field ma_amount_used in message CS5_CCN_GPRS_MA") 
				} 
				def setFn97(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.imei_number = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field imei_number in message CS5_CCN_GPRS_MA") 
				} 
				def setFn98(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.imsi_number = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field imsi_number in message CS5_CCN_GPRS_MA") 
				} 
				def setFn99(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.msg_unique_id_enrich = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field msg_unique_id_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn100(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.base_file_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field base_file_name in message CS5_CCN_GPRS_MA") 
				} 
				def setFn101(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.path = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field path in message CS5_CCN_GPRS_MA") 
				} 
				def setFn102(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.line_number = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field line_number in message CS5_CCN_GPRS_MA") 
				} 
				def setFn103(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.file_id = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field file_id in message CS5_CCN_GPRS_MA") 
				} 
				def setFn104(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.processed_timestamp = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field processed_timestamp in message CS5_CCN_GPRS_MA") 
				} 
				def setFn105(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.ltz_event_timestamp_enrich = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field ltz_event_timestamp_enrich in message CS5_CCN_GPRS_MA") 
				} 
				def setFn106(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.transactionid_pp3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field transactionid_pp3 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn107(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.chargeamount_pp3 = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field chargeamount_pp3 in message CS5_CCN_GPRS_MA") 
				} 
				def setFn108(curObj: CS5_CCN_GPRS_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[scala.Array[Boolean]]) 
				  curObj.kamanja_system_null_flags = value.asInstanceOf[scala.Array[Boolean]]; 
				else if(value.isInstanceOf[scala.Array[_]]) 
				  curObj.kamanja_system_null_flags = value.asInstanceOf[scala.Array[_]].map(v => v.asInstanceOf[Boolean]); 
				 else throw new Exception(s"Value is the not the correct type scala.Array[Boolean]/Array[_] for field kamanja_system_null_flags in message CS5_CCN_GPRS_MA") 
				} 

    val setFnArr = Array[(CS5_CCN_GPRS_MA, Any) => Unit](setFn0,setFn1,setFn2,setFn3,setFn4,setFn5,setFn6,setFn7,setFn8,setFn9,setFn10,setFn11,setFn12,setFn13,setFn14,setFn15,setFn16,setFn17,setFn18,setFn19,setFn20,setFn21,setFn22,setFn23,setFn24,setFn25,setFn26,setFn27,setFn28,setFn29,setFn30,setFn31,setFn32,setFn33,setFn34,setFn35,setFn36,setFn37,setFn38,setFn39,setFn40,setFn41,setFn42,setFn43,setFn44,setFn45,setFn46,setFn47,setFn48,setFn49,setFn50,setFn51,setFn52,setFn53,setFn54,setFn55,setFn56,setFn57,setFn58,setFn59,setFn60,setFn61,setFn62,setFn63,setFn64,setFn65,setFn66,setFn67,setFn68,setFn69,setFn70,setFn71,setFn72,setFn73,setFn74,setFn75,setFn76,setFn77,setFn78,setFn79,setFn80,setFn81,setFn82,setFn83,setFn84,setFn85,setFn86,setFn87,setFn88,setFn89,setFn90,setFn91,setFn92,setFn93,setFn94,setFn95,setFn96,setFn97,setFn98,setFn99,setFn100,setFn101,setFn102,setFn103,setFn104,setFn105,setFn106,setFn107,setFn108    )
		def getFn0(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.switchcalltype.asInstanceOf[AnyRef]; 
		def getFn1(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.chrononumber.asInstanceOf[AnyRef]; 
		def getFn2(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.imsinumber.asInstanceOf[AnyRef]; 
		def getFn3(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.imeinumber.asInstanceOf[AnyRef]; 
		def getFn4(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.mobilenumber.asInstanceOf[AnyRef]; 
		def getFn5(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.calldate.asInstanceOf[AnyRef]; 
		def getFn6(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.chargingid.asInstanceOf[AnyRef]; 
		def getFn7(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.callduration.asInstanceOf[AnyRef]; 
		def getFn8(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.accesspointname.asInstanceOf[AnyRef]; 
		def getFn9(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.pdptype.asInstanceOf[AnyRef]; 
		def getFn10(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.pdpaddress.asInstanceOf[AnyRef]; 
		def getFn11(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.cellid.asInstanceOf[AnyRef]; 
		def getFn12(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.partialtypeindicator.asInstanceOf[AnyRef]; 
		def getFn13(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.callterminationcause.asInstanceOf[AnyRef]; 
		def getFn14(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.chargingcharacteristics.asInstanceOf[AnyRef]; 
		def getFn15(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.sgsnaddress.asInstanceOf[AnyRef]; 
		def getFn16(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.ggsnaddress.asInstanceOf[AnyRef]; 
		def getFn17(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.qosrequested1.asInstanceOf[AnyRef]; 
		def getFn18(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.qosused1.asInstanceOf[AnyRef]; 
		def getFn19(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.datavolumeoutgoing1.asInstanceOf[AnyRef]; 
		def getFn20(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.datavolumeincoming1.asInstanceOf[AnyRef]; 
		def getFn21(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.qosrequested2.asInstanceOf[AnyRef]; 
		def getFn22(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.qosused2.asInstanceOf[AnyRef]; 
		def getFn23(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.datavolumeoutgoing2.asInstanceOf[AnyRef]; 
		def getFn24(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.datavolumeincoming2.asInstanceOf[AnyRef]; 
		def getFn25(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.qosrequested3.asInstanceOf[AnyRef]; 
		def getFn26(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.qosused3.asInstanceOf[AnyRef]; 
		def getFn27(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.datavolumeoutgoing3.asInstanceOf[AnyRef]; 
		def getFn28(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.datavolumeincoming3.asInstanceOf[AnyRef]; 
		def getFn29(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.recordtype.asInstanceOf[AnyRef]; 
		def getFn30(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.amountofsdpbeforesession.asInstanceOf[AnyRef]; 
		def getFn31(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.amountonsdpaftersession.asInstanceOf[AnyRef]; 
		def getFn32(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.costofsession.asInstanceOf[AnyRef]; 
		def getFn33(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.dedicatedaccountvaluebeforecall.asInstanceOf[AnyRef]; 
		def getFn34(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.dedicatedaccountvalueaftercall.asInstanceOf[AnyRef]; 
		def getFn35(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.dedicatedaccountid.asInstanceOf[AnyRef]; 
		def getFn36(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.dedicatedamountused.asInstanceOf[AnyRef]; 
		def getFn37(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.numberofdedicatedaccountused.asInstanceOf[AnyRef]; 
		def getFn38(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.friendsandfamilyindicator.asInstanceOf[AnyRef]; 
		def getFn39(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.friendsandfamilynumber.asInstanceOf[AnyRef]; 
		def getFn40(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.ratedunitsfreeunits.asInstanceOf[AnyRef]; 
		def getFn41(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.ratedunitsdebit.asInstanceOf[AnyRef]; 
		def getFn42(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.ratedunitscredit.asInstanceOf[AnyRef]; 
		def getFn43(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.offerid.asInstanceOf[AnyRef]; 
		def getFn44(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.offertype.asInstanceOf[AnyRef]; 
		def getFn45(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.bonusamount.asInstanceOf[AnyRef]; 
		def getFn46(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.accountgroupid.asInstanceOf[AnyRef]; 
		def getFn47(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.pamserviceid.asInstanceOf[AnyRef]; 
		def getFn48(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.pamclassid.asInstanceOf[AnyRef]; 
		def getFn49(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.selectiontreetype.asInstanceOf[AnyRef]; 
		def getFn50(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.servedaccount.asInstanceOf[AnyRef]; 
		def getFn51(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.servedofferings.asInstanceOf[AnyRef]; 
		def getFn52(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.terminationcause.asInstanceOf[AnyRef]; 
		def getFn53(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.chargingcontextid.asInstanceOf[AnyRef]; 
		def getFn54(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.servicecontextid.asInstanceOf[AnyRef]; 
		def getFn55(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.servicesessionidcallreferencenumber.asInstanceOf[AnyRef]; 
		def getFn56(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.sourcefilename.asInstanceOf[AnyRef]; 
		def getFn57(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.networkcd.asInstanceOf[AnyRef]; 
		def getFn58(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.serviceclassid.asInstanceOf[AnyRef]; 
		def getFn59(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.paytype.asInstanceOf[AnyRef]; 
		def getFn60(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.callreferenceid.asInstanceOf[AnyRef]; 
		def getFn61(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.mainamountused.asInstanceOf[AnyRef]; 
		def getFn62(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.serviceprovider_identifier.asInstanceOf[AnyRef]; 
		def getFn63(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.vascode.asInstanceOf[AnyRef]; 
		def getFn64(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.originatingservices.asInstanceOf[AnyRef]; 
		def getFn65(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.originatingstation.asInstanceOf[AnyRef]; 
		def getFn66(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.terminatingstation.asInstanceOf[AnyRef]; 
		def getFn67(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.subscriptiontype.asInstanceOf[AnyRef]; 
		def getFn68(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.transid.asInstanceOf[AnyRef]; 
		def getFn69(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.channel.asInstanceOf[AnyRef]; 
		def getFn70(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.calledcallingnumber.asInstanceOf[AnyRef]; 
		def getFn71(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.eventtime.asInstanceOf[AnyRef]; 
		def getFn72(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.category.asInstanceOf[AnyRef]; 
		def getFn73(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.sucategory.asInstanceOf[AnyRef]; 
		def getFn74(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.content_name.asInstanceOf[AnyRef]; 
		def getFn75(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.terminating_number.asInstanceOf[AnyRef]; 
		def getFn76(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.compositerecords.asInstanceOf[AnyRef]; 
		def getFn77(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.file_name.asInstanceOf[AnyRef]; 
		def getFn78(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.file_offset.asInstanceOf[AnyRef]; 
		def getFn79(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.kamanja_loaded_date.asInstanceOf[AnyRef]; 
		def getFn80(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.file_mod_date.asInstanceOf[AnyRef]; 
		def getFn81(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.bill_type_enrich.asInstanceOf[AnyRef]; 
		def getFn82(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.network_type_enrich.asInstanceOf[AnyRef]; 
		def getFn83(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.data_type_enrich.asInstanceOf[AnyRef]; 
		def getFn84(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.tac_enrich.asInstanceOf[AnyRef]; 
		def getFn85(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.normalizedchargedpartycgi_enrich.asInstanceOf[AnyRef]; 
		def getFn86(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.latitude_enrich.asInstanceOf[AnyRef]; 
		def getFn87(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.longitude_enrich.asInstanceOf[AnyRef]; 
		def getFn88(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.locationtype_enrich.asInstanceOf[AnyRef]; 
		def getFn89(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.date_key.asInstanceOf[AnyRef]; 
		def getFn90(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.msisdn_key.asInstanceOf[AnyRef]; 
		def getFn91(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.event_timestamp_enrich.asInstanceOf[AnyRef]; 
		def getFn92(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.original_timestamp_enrich.asInstanceOf[AnyRef]; 
		def getFn93(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.is_free_enrich.asInstanceOf[AnyRef]; 
		def getFn94(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.data_volume_incoming_1.asInstanceOf[AnyRef]; 
		def getFn95(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.data_volume_outgoing_1.asInstanceOf[AnyRef]; 
		def getFn96(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.ma_amount_used.asInstanceOf[AnyRef]; 
		def getFn97(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.imei_number.asInstanceOf[AnyRef]; 
		def getFn98(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.imsi_number.asInstanceOf[AnyRef]; 
		def getFn99(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.msg_unique_id_enrich.asInstanceOf[AnyRef]; 
		def getFn100(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.base_file_name.asInstanceOf[AnyRef]; 
		def getFn101(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.path.asInstanceOf[AnyRef]; 
		def getFn102(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.line_number.asInstanceOf[AnyRef]; 
		def getFn103(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.file_id.asInstanceOf[AnyRef]; 
		def getFn104(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.processed_timestamp.asInstanceOf[AnyRef]; 
		def getFn105(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.ltz_event_timestamp_enrich.asInstanceOf[AnyRef]; 
		def getFn106(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.transactionid_pp3.asInstanceOf[AnyRef]; 
		def getFn107(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.chargeamount_pp3.asInstanceOf[AnyRef]; 
		def getFn108(curObj: CS5_CCN_GPRS_MA): AnyRef = curObj.kamanja_system_null_flags.asInstanceOf[AnyRef]; 

    val getFnArr = Array[(CS5_CCN_GPRS_MA) => AnyRef](getFn0,getFn1,getFn2,getFn3,getFn4,getFn5,getFn6,getFn7,getFn8,getFn9,getFn10,getFn11,getFn12,getFn13,getFn14,getFn15,getFn16,getFn17,getFn18,getFn19,getFn20,getFn21,getFn22,getFn23,getFn24,getFn25,getFn26,getFn27,getFn28,getFn29,getFn30,getFn31,getFn32,getFn33,getFn34,getFn35,getFn36,getFn37,getFn38,getFn39,getFn40,getFn41,getFn42,getFn43,getFn44,getFn45,getFn46,getFn47,getFn48,getFn49,getFn50,getFn51,getFn52,getFn53,getFn54,getFn55,getFn56,getFn57,getFn58,getFn59,getFn60,getFn61,getFn62,getFn63,getFn64,getFn65,getFn66,getFn67,getFn68,getFn69,getFn70,getFn71,getFn72,getFn73,getFn74,getFn75,getFn76,getFn77,getFn78,getFn79,getFn80,getFn81,getFn82,getFn83,getFn84,getFn85,getFn86,getFn87,getFn88,getFn89,getFn90,getFn91,getFn92,getFn93,getFn94,getFn95,getFn96,getFn97,getFn98,getFn99,getFn100,getFn101,getFn102,getFn103,getFn104,getFn105,getFn106,getFn107,getFn108    )



    override def getRDDObject(): RDDObject[ContainerInterface] = CS5_CCN_GPRS_MA.asInstanceOf[RDDObject[ContainerInterface]];


    
    final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);
      
    override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
      try {
        if (oldVerobj == null) return null;
        oldVerobj match {
          
      case oldVerobj: com.mtn.messages.CS5_CCN_GPRS_MA => { return  convertToVer1000000(oldVerobj); } 
          case _ => {
            throw new Exception("Unhandled Version Found");
          }
        }
      } catch {
        case e: Exception => {
          throw e
        }
      }
      return null;
    }
  
    private def convertToVer1000000(oldVerobj: com.mtn.messages.CS5_CCN_GPRS_MA): com.mtn.messages.CS5_CCN_GPRS_MA= {
      return oldVerobj
    }
  
      
  /****   DEPRECATED METHODS ***/
  override def FullName: String = getFullTypeName
  override def NameSpace: String = getTypeNameSpace
  override def Name: String = getTypeName
  override def Version: String = getTypeVersion
  override def CreateNewMessage: BaseMsg= createInstance.asInstanceOf[BaseMsg];
  override def CreateNewContainer: BaseContainer= null;
  override def IsFixed: Boolean = true
  override def IsKv: Boolean = false
  override def CanPersist: Boolean = false
  override def isMessage: Boolean = true
  override def isContainer: Boolean = false
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj CS5_CCN_GPRS_MA") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj CS5_CCN_GPRS_MA");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj CS5_CCN_GPRS_MA");
 override def NeedToTransformData: Boolean = false
    }

class CS5_CCN_GPRS_MA(factory: MessageFactoryInterface, other: CS5_CCN_GPRS_MA) extends MessageInterface(factory) { 
 
  val log = CS5_CCN_GPRS_MA.log

      var attributeTypes = CS5_CCN_GPRS_MA.attributeTypes
      
		 var keyTypes = CS5_CCN_GPRS_MA.keyTypes;
    
     if (other != null && other != this) {
      // call copying fields from other to local variables
      fromFunc(other)
    }
    
    override def save: Unit = { CS5_CCN_GPRS_MA.saveOne(this) }
  
    def Clone(): ContainerOrConcept = { CS5_CCN_GPRS_MA.build(this) }

		override def getPartitionKey: Array[String] = {
		var partitionKeys: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
		try {
		 partitionKeys += com.ligadata.BaseTypes.LongImpl.toString(get(caseSensitiveKey("msisdn_key")).asInstanceOf[Long]);
		 }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      		 partitionKeys.toArray; 

 		} 
 

		override def getPrimaryKey: Array[String] = Array[String]() 

    override def getAttributeType(name: String): AttributeTypeInfo = {
      if (name == null || name.trim() == "") return null;
      keyTypes.getOrElse(caseSensitiveKey(name), null)
    }
  
  
      var setFnArr = CS5_CCN_GPRS_MA.setFnArr
    
      var getFnArr = CS5_CCN_GPRS_MA.getFnArr
    
 		var switchcalltype : String = _; 
 		var chrononumber : String = _; 
 		var imsinumber : String = _; 
 		var imeinumber : String = _; 
 		var mobilenumber : String = _; 
 		var calldate : Long = _; 
 		var chargingid : String = _; 
 		var callduration : String = _; 
 		var accesspointname : String = _; 
 		var pdptype : String = _; 
 		var pdpaddress : String = _; 
 		var cellid : String = _; 
 		var partialtypeindicator : String = _; 
 		var callterminationcause : String = _; 
 		var chargingcharacteristics : String = _; 
 		var sgsnaddress : String = _; 
 		var ggsnaddress : String = _; 
 		var qosrequested1 : String = _; 
 		var qosused1 : String = _; 
 		var datavolumeoutgoing1 : String = _; 
 		var datavolumeincoming1 : String = _; 
 		var qosrequested2 : String = _; 
 		var qosused2 : String = _; 
 		var datavolumeoutgoing2 : String = _; 
 		var datavolumeincoming2 : String = _; 
 		var qosrequested3 : String = _; 
 		var qosused3 : String = _; 
 		var datavolumeoutgoing3 : String = _; 
 		var datavolumeincoming3 : String = _; 
 		var recordtype : String = _; 
 		var amountofsdpbeforesession : String = _; 
 		var amountonsdpaftersession : String = _; 
 		var costofsession : String = _; 
 		var dedicatedaccountvaluebeforecall : String = _; 
 		var dedicatedaccountvalueaftercall : String = _; 
 		var dedicatedaccountid : String = _; 
 		var dedicatedamountused : String = _; 
 		var numberofdedicatedaccountused : String = _; 
 		var friendsandfamilyindicator : String = _; 
 		var friendsandfamilynumber : String = _; 
 		var ratedunitsfreeunits : String = _; 
 		var ratedunitsdebit : String = _; 
 		var ratedunitscredit : String = _; 
 		var offerid : String = _; 
 		var offertype : String = _; 
 		var bonusamount : String = _; 
 		var accountgroupid : String = _; 
 		var pamserviceid : String = _; 
 		var pamclassid : String = _; 
 		var selectiontreetype : String = _; 
 		var servedaccount : String = _; 
 		var servedofferings : String = _; 
 		var terminationcause : String = _; 
 		var chargingcontextid : String = _; 
 		var servicecontextid : String = _; 
 		var servicesessionidcallreferencenumber : String = _; 
 		var sourcefilename : String = _; 
 		var networkcd : String = _; 
 		var serviceclassid : String = _; 
 		var paytype : String = _; 
 		var callreferenceid : String = _; 
 		var mainamountused : String = _; 
 		var serviceprovider_identifier : String = _; 
 		var vascode : String = _; 
 		var originatingservices : String = _; 
 		var originatingstation : String = _; 
 		var terminatingstation : String = _; 
 		var subscriptiontype : String = _; 
 		var transid : String = _; 
 		var channel : String = _; 
 		var calledcallingnumber : String = _; 
 		var eventtime : String = _; 
 		var category : String = _; 
 		var sucategory : String = _; 
 		var content_name : String = _; 
 		var terminating_number : String = _; 
 		var compositerecords : String = _; 
 		var file_name : String = _; 
 		var file_offset : Long = _; 
 		var kamanja_loaded_date : String = _; 
 		var file_mod_date : String = _; 
 		var bill_type_enrich : Int = _; 
 		var network_type_enrich : Int = _; 
 		var data_type_enrich : Int = _; 
 		var tac_enrich : String = _; 
 		var normalizedchargedpartycgi_enrich : String = _; 
 		var latitude_enrich : Double = _; 
 		var longitude_enrich : Double = _; 
 		var locationtype_enrich : Int = _; 
 		var date_key : Int = _; 
 		var msisdn_key : Long = _; 
 		var event_timestamp_enrich : Long = _; 
 		var original_timestamp_enrich : String = _; 
 		var is_free_enrich : Int = _; 
 		var data_volume_incoming_1 : Long = _; 
 		var data_volume_outgoing_1 : Long = _; 
 		var ma_amount_used : Double = _; 
 		var imei_number : String = _; 
 		var imsi_number : String = _; 
 		var msg_unique_id_enrich : Long = _; 
 		var base_file_name : String = _; 
 		var path : String = _; 
 		var line_number : Long = _; 
 		var file_id : String = _; 
 		var processed_timestamp : Long = _; 
 		var ltz_event_timestamp_enrich : Long = _; 
 		var transactionid_pp3 : String = _; 
 		var chargeamount_pp3 : Double = _; 
 		var kamanja_system_null_flags : scala.Array[Boolean] = _; 

    override def getAttributeTypes(): Array[AttributeTypeInfo] = {
      if (attributeTypes == null) return null;
      return attributeTypes
    }
    
    private def getWithReflection(keyName: String): AnyRef = {
      if(keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
      val ru = scala.reflect.runtime.universe
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val im = m.reflect(this)
      val fieldX = ru.typeOf[CS5_CCN_GPRS_MA].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
      val fmX = im.reflectField(fieldX)
      return fmX.get.asInstanceOf[AnyRef];      
    } 
   
    override def get(key: String): AnyRef = {
    try {
      // Try with reflection
      return getByName(caseSensitiveKey(key))
    } catch {
      case e: Exception => {
        val stackTrace = StackTrace.ThrowableTraceString(e)
        log.debug("StackTrace:" + stackTrace)
        // Call By Name
        return getWithReflection(caseSensitiveKey(key))
        }
      }
    }      
    
    private def getByName(keyName: String): AnyRef = {
     if(keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
   
      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container CS5_CCN_GPRS_MA", null);
      return get(keyTypes(key).getIndex)
  }

    private def getOrElseByName(key: String, defaultVal: Any): AnyRef = {
     if(key == null) throw new Exception("Please provide proper key name "+key);
      if (!keyTypes.contains(key)) {
          if(defaultVal == null) return null;
          return defaultVal.asInstanceOf[AnyRef];
      }
      return get(keyTypes(key).getIndex)
  }

  
    override def getOrElse(keyName: String, defaultVal: Any): AnyRef = { // Return (value)
      if (keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
      try {
        return getOrElseByName(key, defaultVal)
       } catch {
        case e: Exception => {
          log.debug("", e)
          if(defaultVal == null) return null;
          return defaultVal.asInstanceOf[AnyRef];
        }
      }
      return null;
    }
   
    override def get(index : Int) : AnyRef = {
      if (index >= 0 && index < getFnArr.length) {
        return getFnArr(index)(this)
      }
      throw new Exception(s"$index is out of range for message CS5_CCN_GPRS_MA");
    }
    
    override def getOrElse(index: Int, defaultVal: Any): AnyRef = { // Return (value)
      try {
        return get(index);
        } catch {
        case e: Exception => {
          log.debug("", e)
          if(defaultVal == null) return null;
          return defaultVal.asInstanceOf[AnyRef];
        }
      }
      return null;
    }
  
    override def getAttributeNames(): Array[String] = {
      try {
        if (keyTypes.isEmpty) {
          return null;
          } else {
          return keyTypes.keySet.toArray;
        }
      } catch {
        case e: Exception => {
          log.debug("", e)
          throw e
        }
      }
      return null;
    }
 
    override def getAllAttributeValues(): Array[AttributeValue] = { // Has ( value, attributetypeinfo))
      var attributeVals = new Array[AttributeValue](109);
      try{
 				attributeVals(0) = new AttributeValue(this.switchcalltype, attributeTypes(0)) 
				attributeVals(1) = new AttributeValue(this.chrononumber, attributeTypes(1)) 
				attributeVals(2) = new AttributeValue(this.imsinumber, attributeTypes(2)) 
				attributeVals(3) = new AttributeValue(this.imeinumber, attributeTypes(3)) 
				attributeVals(4) = new AttributeValue(this.mobilenumber, attributeTypes(4)) 
				attributeVals(5) = new AttributeValue(this.calldate, attributeTypes(5)) 
				attributeVals(6) = new AttributeValue(this.chargingid, attributeTypes(6)) 
				attributeVals(7) = new AttributeValue(this.callduration, attributeTypes(7)) 
				attributeVals(8) = new AttributeValue(this.accesspointname, attributeTypes(8)) 
				attributeVals(9) = new AttributeValue(this.pdptype, attributeTypes(9)) 
				attributeVals(10) = new AttributeValue(this.pdpaddress, attributeTypes(10)) 
				attributeVals(11) = new AttributeValue(this.cellid, attributeTypes(11)) 
				attributeVals(12) = new AttributeValue(this.partialtypeindicator, attributeTypes(12)) 
				attributeVals(13) = new AttributeValue(this.callterminationcause, attributeTypes(13)) 
				attributeVals(14) = new AttributeValue(this.chargingcharacteristics, attributeTypes(14)) 
				attributeVals(15) = new AttributeValue(this.sgsnaddress, attributeTypes(15)) 
				attributeVals(16) = new AttributeValue(this.ggsnaddress, attributeTypes(16)) 
				attributeVals(17) = new AttributeValue(this.qosrequested1, attributeTypes(17)) 
				attributeVals(18) = new AttributeValue(this.qosused1, attributeTypes(18)) 
				attributeVals(19) = new AttributeValue(this.datavolumeoutgoing1, attributeTypes(19)) 
				attributeVals(20) = new AttributeValue(this.datavolumeincoming1, attributeTypes(20)) 
				attributeVals(21) = new AttributeValue(this.qosrequested2, attributeTypes(21)) 
				attributeVals(22) = new AttributeValue(this.qosused2, attributeTypes(22)) 
				attributeVals(23) = new AttributeValue(this.datavolumeoutgoing2, attributeTypes(23)) 
				attributeVals(24) = new AttributeValue(this.datavolumeincoming2, attributeTypes(24)) 
				attributeVals(25) = new AttributeValue(this.qosrequested3, attributeTypes(25)) 
				attributeVals(26) = new AttributeValue(this.qosused3, attributeTypes(26)) 
				attributeVals(27) = new AttributeValue(this.datavolumeoutgoing3, attributeTypes(27)) 
				attributeVals(28) = new AttributeValue(this.datavolumeincoming3, attributeTypes(28)) 
				attributeVals(29) = new AttributeValue(this.recordtype, attributeTypes(29)) 
				attributeVals(30) = new AttributeValue(this.amountofsdpbeforesession, attributeTypes(30)) 
				attributeVals(31) = new AttributeValue(this.amountonsdpaftersession, attributeTypes(31)) 
				attributeVals(32) = new AttributeValue(this.costofsession, attributeTypes(32)) 
				attributeVals(33) = new AttributeValue(this.dedicatedaccountvaluebeforecall, attributeTypes(33)) 
				attributeVals(34) = new AttributeValue(this.dedicatedaccountvalueaftercall, attributeTypes(34)) 
				attributeVals(35) = new AttributeValue(this.dedicatedaccountid, attributeTypes(35)) 
				attributeVals(36) = new AttributeValue(this.dedicatedamountused, attributeTypes(36)) 
				attributeVals(37) = new AttributeValue(this.numberofdedicatedaccountused, attributeTypes(37)) 
				attributeVals(38) = new AttributeValue(this.friendsandfamilyindicator, attributeTypes(38)) 
				attributeVals(39) = new AttributeValue(this.friendsandfamilynumber, attributeTypes(39)) 
				attributeVals(40) = new AttributeValue(this.ratedunitsfreeunits, attributeTypes(40)) 
				attributeVals(41) = new AttributeValue(this.ratedunitsdebit, attributeTypes(41)) 
				attributeVals(42) = new AttributeValue(this.ratedunitscredit, attributeTypes(42)) 
				attributeVals(43) = new AttributeValue(this.offerid, attributeTypes(43)) 
				attributeVals(44) = new AttributeValue(this.offertype, attributeTypes(44)) 
				attributeVals(45) = new AttributeValue(this.bonusamount, attributeTypes(45)) 
				attributeVals(46) = new AttributeValue(this.accountgroupid, attributeTypes(46)) 
				attributeVals(47) = new AttributeValue(this.pamserviceid, attributeTypes(47)) 
				attributeVals(48) = new AttributeValue(this.pamclassid, attributeTypes(48)) 
				attributeVals(49) = new AttributeValue(this.selectiontreetype, attributeTypes(49)) 
				attributeVals(50) = new AttributeValue(this.servedaccount, attributeTypes(50)) 
				attributeVals(51) = new AttributeValue(this.servedofferings, attributeTypes(51)) 
				attributeVals(52) = new AttributeValue(this.terminationcause, attributeTypes(52)) 
				attributeVals(53) = new AttributeValue(this.chargingcontextid, attributeTypes(53)) 
				attributeVals(54) = new AttributeValue(this.servicecontextid, attributeTypes(54)) 
				attributeVals(55) = new AttributeValue(this.servicesessionidcallreferencenumber, attributeTypes(55)) 
				attributeVals(56) = new AttributeValue(this.sourcefilename, attributeTypes(56)) 
				attributeVals(57) = new AttributeValue(this.networkcd, attributeTypes(57)) 
				attributeVals(58) = new AttributeValue(this.serviceclassid, attributeTypes(58)) 
				attributeVals(59) = new AttributeValue(this.paytype, attributeTypes(59)) 
				attributeVals(60) = new AttributeValue(this.callreferenceid, attributeTypes(60)) 
				attributeVals(61) = new AttributeValue(this.mainamountused, attributeTypes(61)) 
				attributeVals(62) = new AttributeValue(this.serviceprovider_identifier, attributeTypes(62)) 
				attributeVals(63) = new AttributeValue(this.vascode, attributeTypes(63)) 
				attributeVals(64) = new AttributeValue(this.originatingservices, attributeTypes(64)) 
				attributeVals(65) = new AttributeValue(this.originatingstation, attributeTypes(65)) 
				attributeVals(66) = new AttributeValue(this.terminatingstation, attributeTypes(66)) 
				attributeVals(67) = new AttributeValue(this.subscriptiontype, attributeTypes(67)) 
				attributeVals(68) = new AttributeValue(this.transid, attributeTypes(68)) 
				attributeVals(69) = new AttributeValue(this.channel, attributeTypes(69)) 
				attributeVals(70) = new AttributeValue(this.calledcallingnumber, attributeTypes(70)) 
				attributeVals(71) = new AttributeValue(this.eventtime, attributeTypes(71)) 
				attributeVals(72) = new AttributeValue(this.category, attributeTypes(72)) 
				attributeVals(73) = new AttributeValue(this.sucategory, attributeTypes(73)) 
				attributeVals(74) = new AttributeValue(this.content_name, attributeTypes(74)) 
				attributeVals(75) = new AttributeValue(this.terminating_number, attributeTypes(75)) 
				attributeVals(76) = new AttributeValue(this.compositerecords, attributeTypes(76)) 
				attributeVals(77) = new AttributeValue(this.file_name, attributeTypes(77)) 
				attributeVals(78) = new AttributeValue(this.file_offset, attributeTypes(78)) 
				attributeVals(79) = new AttributeValue(this.kamanja_loaded_date, attributeTypes(79)) 
				attributeVals(80) = new AttributeValue(this.file_mod_date, attributeTypes(80)) 
				attributeVals(81) = new AttributeValue(this.bill_type_enrich, attributeTypes(81)) 
				attributeVals(82) = new AttributeValue(this.network_type_enrich, attributeTypes(82)) 
				attributeVals(83) = new AttributeValue(this.data_type_enrich, attributeTypes(83)) 
				attributeVals(84) = new AttributeValue(this.tac_enrich, attributeTypes(84)) 
				attributeVals(85) = new AttributeValue(this.normalizedchargedpartycgi_enrich, attributeTypes(85)) 
				attributeVals(86) = new AttributeValue(this.latitude_enrich, attributeTypes(86)) 
				attributeVals(87) = new AttributeValue(this.longitude_enrich, attributeTypes(87)) 
				attributeVals(88) = new AttributeValue(this.locationtype_enrich, attributeTypes(88)) 
				attributeVals(89) = new AttributeValue(this.date_key, attributeTypes(89)) 
				attributeVals(90) = new AttributeValue(this.msisdn_key, attributeTypes(90)) 
				attributeVals(91) = new AttributeValue(this.event_timestamp_enrich, attributeTypes(91)) 
				attributeVals(92) = new AttributeValue(this.original_timestamp_enrich, attributeTypes(92)) 
				attributeVals(93) = new AttributeValue(this.is_free_enrich, attributeTypes(93)) 
				attributeVals(94) = new AttributeValue(this.data_volume_incoming_1, attributeTypes(94)) 
				attributeVals(95) = new AttributeValue(this.data_volume_outgoing_1, attributeTypes(95)) 
				attributeVals(96) = new AttributeValue(this.ma_amount_used, attributeTypes(96)) 
				attributeVals(97) = new AttributeValue(this.imei_number, attributeTypes(97)) 
				attributeVals(98) = new AttributeValue(this.imsi_number, attributeTypes(98)) 
				attributeVals(99) = new AttributeValue(this.msg_unique_id_enrich, attributeTypes(99)) 
				attributeVals(100) = new AttributeValue(this.base_file_name, attributeTypes(100)) 
				attributeVals(101) = new AttributeValue(this.path, attributeTypes(101)) 
				attributeVals(102) = new AttributeValue(this.line_number, attributeTypes(102)) 
				attributeVals(103) = new AttributeValue(this.file_id, attributeTypes(103)) 
				attributeVals(104) = new AttributeValue(this.processed_timestamp, attributeTypes(104)) 
				attributeVals(105) = new AttributeValue(this.ltz_event_timestamp_enrich, attributeTypes(105)) 
				attributeVals(106) = new AttributeValue(this.transactionid_pp3, attributeTypes(106)) 
				attributeVals(107) = new AttributeValue(this.chargeamount_pp3, attributeTypes(107)) 
				attributeVals(108) = new AttributeValue(this.kamanja_system_null_flags, attributeTypes(108)) 
       
      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return attributeVals;
    }      
    
    override def getOnlyValuesForAllAttributes(): Array[Object] = {
      var allVals = new Array[Object](109);
      try{
 				allVals(0) = this.switchcalltype.asInstanceOf[AnyRef]; 
				allVals(1) = this.chrononumber.asInstanceOf[AnyRef]; 
				allVals(2) = this.imsinumber.asInstanceOf[AnyRef]; 
				allVals(3) = this.imeinumber.asInstanceOf[AnyRef]; 
				allVals(4) = this.mobilenumber.asInstanceOf[AnyRef]; 
				allVals(5) = this.calldate.asInstanceOf[AnyRef]; 
				allVals(6) = this.chargingid.asInstanceOf[AnyRef]; 
				allVals(7) = this.callduration.asInstanceOf[AnyRef]; 
				allVals(8) = this.accesspointname.asInstanceOf[AnyRef]; 
				allVals(9) = this.pdptype.asInstanceOf[AnyRef]; 
				allVals(10) = this.pdpaddress.asInstanceOf[AnyRef]; 
				allVals(11) = this.cellid.asInstanceOf[AnyRef]; 
				allVals(12) = this.partialtypeindicator.asInstanceOf[AnyRef]; 
				allVals(13) = this.callterminationcause.asInstanceOf[AnyRef]; 
				allVals(14) = this.chargingcharacteristics.asInstanceOf[AnyRef]; 
				allVals(15) = this.sgsnaddress.asInstanceOf[AnyRef]; 
				allVals(16) = this.ggsnaddress.asInstanceOf[AnyRef]; 
				allVals(17) = this.qosrequested1.asInstanceOf[AnyRef]; 
				allVals(18) = this.qosused1.asInstanceOf[AnyRef]; 
				allVals(19) = this.datavolumeoutgoing1.asInstanceOf[AnyRef]; 
				allVals(20) = this.datavolumeincoming1.asInstanceOf[AnyRef]; 
				allVals(21) = this.qosrequested2.asInstanceOf[AnyRef]; 
				allVals(22) = this.qosused2.asInstanceOf[AnyRef]; 
				allVals(23) = this.datavolumeoutgoing2.asInstanceOf[AnyRef]; 
				allVals(24) = this.datavolumeincoming2.asInstanceOf[AnyRef]; 
				allVals(25) = this.qosrequested3.asInstanceOf[AnyRef]; 
				allVals(26) = this.qosused3.asInstanceOf[AnyRef]; 
				allVals(27) = this.datavolumeoutgoing3.asInstanceOf[AnyRef]; 
				allVals(28) = this.datavolumeincoming3.asInstanceOf[AnyRef]; 
				allVals(29) = this.recordtype.asInstanceOf[AnyRef]; 
				allVals(30) = this.amountofsdpbeforesession.asInstanceOf[AnyRef]; 
				allVals(31) = this.amountonsdpaftersession.asInstanceOf[AnyRef]; 
				allVals(32) = this.costofsession.asInstanceOf[AnyRef]; 
				allVals(33) = this.dedicatedaccountvaluebeforecall.asInstanceOf[AnyRef]; 
				allVals(34) = this.dedicatedaccountvalueaftercall.asInstanceOf[AnyRef]; 
				allVals(35) = this.dedicatedaccountid.asInstanceOf[AnyRef]; 
				allVals(36) = this.dedicatedamountused.asInstanceOf[AnyRef]; 
				allVals(37) = this.numberofdedicatedaccountused.asInstanceOf[AnyRef]; 
				allVals(38) = this.friendsandfamilyindicator.asInstanceOf[AnyRef]; 
				allVals(39) = this.friendsandfamilynumber.asInstanceOf[AnyRef]; 
				allVals(40) = this.ratedunitsfreeunits.asInstanceOf[AnyRef]; 
				allVals(41) = this.ratedunitsdebit.asInstanceOf[AnyRef]; 
				allVals(42) = this.ratedunitscredit.asInstanceOf[AnyRef]; 
				allVals(43) = this.offerid.asInstanceOf[AnyRef]; 
				allVals(44) = this.offertype.asInstanceOf[AnyRef]; 
				allVals(45) = this.bonusamount.asInstanceOf[AnyRef]; 
				allVals(46) = this.accountgroupid.asInstanceOf[AnyRef]; 
				allVals(47) = this.pamserviceid.asInstanceOf[AnyRef]; 
				allVals(48) = this.pamclassid.asInstanceOf[AnyRef]; 
				allVals(49) = this.selectiontreetype.asInstanceOf[AnyRef]; 
				allVals(50) = this.servedaccount.asInstanceOf[AnyRef]; 
				allVals(51) = this.servedofferings.asInstanceOf[AnyRef]; 
				allVals(52) = this.terminationcause.asInstanceOf[AnyRef]; 
				allVals(53) = this.chargingcontextid.asInstanceOf[AnyRef]; 
				allVals(54) = this.servicecontextid.asInstanceOf[AnyRef]; 
				allVals(55) = this.servicesessionidcallreferencenumber.asInstanceOf[AnyRef]; 
				allVals(56) = this.sourcefilename.asInstanceOf[AnyRef]; 
				allVals(57) = this.networkcd.asInstanceOf[AnyRef]; 
				allVals(58) = this.serviceclassid.asInstanceOf[AnyRef]; 
				allVals(59) = this.paytype.asInstanceOf[AnyRef]; 
				allVals(60) = this.callreferenceid.asInstanceOf[AnyRef]; 
				allVals(61) = this.mainamountused.asInstanceOf[AnyRef]; 
				allVals(62) = this.serviceprovider_identifier.asInstanceOf[AnyRef]; 
				allVals(63) = this.vascode.asInstanceOf[AnyRef]; 
				allVals(64) = this.originatingservices.asInstanceOf[AnyRef]; 
				allVals(65) = this.originatingstation.asInstanceOf[AnyRef]; 
				allVals(66) = this.terminatingstation.asInstanceOf[AnyRef]; 
				allVals(67) = this.subscriptiontype.asInstanceOf[AnyRef]; 
				allVals(68) = this.transid.asInstanceOf[AnyRef]; 
				allVals(69) = this.channel.asInstanceOf[AnyRef]; 
				allVals(70) = this.calledcallingnumber.asInstanceOf[AnyRef]; 
				allVals(71) = this.eventtime.asInstanceOf[AnyRef]; 
				allVals(72) = this.category.asInstanceOf[AnyRef]; 
				allVals(73) = this.sucategory.asInstanceOf[AnyRef]; 
				allVals(74) = this.content_name.asInstanceOf[AnyRef]; 
				allVals(75) = this.terminating_number.asInstanceOf[AnyRef]; 
				allVals(76) = this.compositerecords.asInstanceOf[AnyRef]; 
				allVals(77) = this.file_name.asInstanceOf[AnyRef]; 
				allVals(78) = this.file_offset.asInstanceOf[AnyRef]; 
				allVals(79) = this.kamanja_loaded_date.asInstanceOf[AnyRef]; 
				allVals(80) = this.file_mod_date.asInstanceOf[AnyRef]; 
				allVals(81) = this.bill_type_enrich.asInstanceOf[AnyRef]; 
				allVals(82) = this.network_type_enrich.asInstanceOf[AnyRef]; 
				allVals(83) = this.data_type_enrich.asInstanceOf[AnyRef]; 
				allVals(84) = this.tac_enrich.asInstanceOf[AnyRef]; 
				allVals(85) = this.normalizedchargedpartycgi_enrich.asInstanceOf[AnyRef]; 
				allVals(86) = this.latitude_enrich.asInstanceOf[AnyRef]; 
				allVals(87) = this.longitude_enrich.asInstanceOf[AnyRef]; 
				allVals(88) = this.locationtype_enrich.asInstanceOf[AnyRef]; 
				allVals(89) = this.date_key.asInstanceOf[AnyRef]; 
				allVals(90) = this.msisdn_key.asInstanceOf[AnyRef]; 
				allVals(91) = this.event_timestamp_enrich.asInstanceOf[AnyRef]; 
				allVals(92) = this.original_timestamp_enrich.asInstanceOf[AnyRef]; 
				allVals(93) = this.is_free_enrich.asInstanceOf[AnyRef]; 
				allVals(94) = this.data_volume_incoming_1.asInstanceOf[AnyRef]; 
				allVals(95) = this.data_volume_outgoing_1.asInstanceOf[AnyRef]; 
				allVals(96) = this.ma_amount_used.asInstanceOf[AnyRef]; 
				allVals(97) = this.imei_number.asInstanceOf[AnyRef]; 
				allVals(98) = this.imsi_number.asInstanceOf[AnyRef]; 
				allVals(99) = this.msg_unique_id_enrich.asInstanceOf[AnyRef]; 
				allVals(100) = this.base_file_name.asInstanceOf[AnyRef]; 
				allVals(101) = this.path.asInstanceOf[AnyRef]; 
				allVals(102) = this.line_number.asInstanceOf[AnyRef]; 
				allVals(103) = this.file_id.asInstanceOf[AnyRef]; 
				allVals(104) = this.processed_timestamp.asInstanceOf[AnyRef]; 
				allVals(105) = this.ltz_event_timestamp_enrich.asInstanceOf[AnyRef]; 
				allVals(106) = this.transactionid_pp3.asInstanceOf[AnyRef]; 
				allVals(107) = this.chargeamount_pp3.asInstanceOf[AnyRef]; 
				allVals(108) = this.kamanja_system_null_flags.asInstanceOf[AnyRef]; 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return allVals;
    }
    
    override def getAllAttributeTypesAndValues(): AllAttributeTypesAndValues = new AllAttributeTypesAndValues(getAttributeTypes, getOnlyValuesForAllAttributes)
    
    override def set(keyName: String, value: Any) = {
      if(keyName == null || keyName.trim.size == 0) throw new Exception("Please provide proper key name "+keyName);
      val key = caseSensitiveKey(keyName);
      try {
   
  			 if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message CS5_CCN_GPRS_MA", null)
			 set(keyTypes(key).getIndex, value); 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
    }
  
      
    def set(index : Int, value :Any): Unit = {
      if (value == null) throw new Exception(s"Value is null for index $index in message CS5_CCN_GPRS_MA ")
      if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message CS5_CCN_GPRS_MA ")
      setFnArr(index)(this, value)
    }
    
    override def set(key: String, value: Any, valTyp: String) = {
      throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
    }
  
    private def fromFunc(other: CS5_CCN_GPRS_MA): CS5_CCN_GPRS_MA = {  
   			this.switchcalltype = com.ligadata.BaseTypes.StringImpl.Clone(other.switchcalltype);
			this.chrononumber = com.ligadata.BaseTypes.StringImpl.Clone(other.chrononumber);
			this.imsinumber = com.ligadata.BaseTypes.StringImpl.Clone(other.imsinumber);
			this.imeinumber = com.ligadata.BaseTypes.StringImpl.Clone(other.imeinumber);
			this.mobilenumber = com.ligadata.BaseTypes.StringImpl.Clone(other.mobilenumber);
			this.calldate = com.ligadata.BaseTypes.LongImpl.Clone(other.calldate);
			this.chargingid = com.ligadata.BaseTypes.StringImpl.Clone(other.chargingid);
			this.callduration = com.ligadata.BaseTypes.StringImpl.Clone(other.callduration);
			this.accesspointname = com.ligadata.BaseTypes.StringImpl.Clone(other.accesspointname);
			this.pdptype = com.ligadata.BaseTypes.StringImpl.Clone(other.pdptype);
			this.pdpaddress = com.ligadata.BaseTypes.StringImpl.Clone(other.pdpaddress);
			this.cellid = com.ligadata.BaseTypes.StringImpl.Clone(other.cellid);
			this.partialtypeindicator = com.ligadata.BaseTypes.StringImpl.Clone(other.partialtypeindicator);
			this.callterminationcause = com.ligadata.BaseTypes.StringImpl.Clone(other.callterminationcause);
			this.chargingcharacteristics = com.ligadata.BaseTypes.StringImpl.Clone(other.chargingcharacteristics);
			this.sgsnaddress = com.ligadata.BaseTypes.StringImpl.Clone(other.sgsnaddress);
			this.ggsnaddress = com.ligadata.BaseTypes.StringImpl.Clone(other.ggsnaddress);
			this.qosrequested1 = com.ligadata.BaseTypes.StringImpl.Clone(other.qosrequested1);
			this.qosused1 = com.ligadata.BaseTypes.StringImpl.Clone(other.qosused1);
			this.datavolumeoutgoing1 = com.ligadata.BaseTypes.StringImpl.Clone(other.datavolumeoutgoing1);
			this.datavolumeincoming1 = com.ligadata.BaseTypes.StringImpl.Clone(other.datavolumeincoming1);
			this.qosrequested2 = com.ligadata.BaseTypes.StringImpl.Clone(other.qosrequested2);
			this.qosused2 = com.ligadata.BaseTypes.StringImpl.Clone(other.qosused2);
			this.datavolumeoutgoing2 = com.ligadata.BaseTypes.StringImpl.Clone(other.datavolumeoutgoing2);
			this.datavolumeincoming2 = com.ligadata.BaseTypes.StringImpl.Clone(other.datavolumeincoming2);
			this.qosrequested3 = com.ligadata.BaseTypes.StringImpl.Clone(other.qosrequested3);
			this.qosused3 = com.ligadata.BaseTypes.StringImpl.Clone(other.qosused3);
			this.datavolumeoutgoing3 = com.ligadata.BaseTypes.StringImpl.Clone(other.datavolumeoutgoing3);
			this.datavolumeincoming3 = com.ligadata.BaseTypes.StringImpl.Clone(other.datavolumeincoming3);
			this.recordtype = com.ligadata.BaseTypes.StringImpl.Clone(other.recordtype);
			this.amountofsdpbeforesession = com.ligadata.BaseTypes.StringImpl.Clone(other.amountofsdpbeforesession);
			this.amountonsdpaftersession = com.ligadata.BaseTypes.StringImpl.Clone(other.amountonsdpaftersession);
			this.costofsession = com.ligadata.BaseTypes.StringImpl.Clone(other.costofsession);
			this.dedicatedaccountvaluebeforecall = com.ligadata.BaseTypes.StringImpl.Clone(other.dedicatedaccountvaluebeforecall);
			this.dedicatedaccountvalueaftercall = com.ligadata.BaseTypes.StringImpl.Clone(other.dedicatedaccountvalueaftercall);
			this.dedicatedaccountid = com.ligadata.BaseTypes.StringImpl.Clone(other.dedicatedaccountid);
			this.dedicatedamountused = com.ligadata.BaseTypes.StringImpl.Clone(other.dedicatedamountused);
			this.numberofdedicatedaccountused = com.ligadata.BaseTypes.StringImpl.Clone(other.numberofdedicatedaccountused);
			this.friendsandfamilyindicator = com.ligadata.BaseTypes.StringImpl.Clone(other.friendsandfamilyindicator);
			this.friendsandfamilynumber = com.ligadata.BaseTypes.StringImpl.Clone(other.friendsandfamilynumber);
			this.ratedunitsfreeunits = com.ligadata.BaseTypes.StringImpl.Clone(other.ratedunitsfreeunits);
			this.ratedunitsdebit = com.ligadata.BaseTypes.StringImpl.Clone(other.ratedunitsdebit);
			this.ratedunitscredit = com.ligadata.BaseTypes.StringImpl.Clone(other.ratedunitscredit);
			this.offerid = com.ligadata.BaseTypes.StringImpl.Clone(other.offerid);
			this.offertype = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype);
			this.bonusamount = com.ligadata.BaseTypes.StringImpl.Clone(other.bonusamount);
			this.accountgroupid = com.ligadata.BaseTypes.StringImpl.Clone(other.accountgroupid);
			this.pamserviceid = com.ligadata.BaseTypes.StringImpl.Clone(other.pamserviceid);
			this.pamclassid = com.ligadata.BaseTypes.StringImpl.Clone(other.pamclassid);
			this.selectiontreetype = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreetype);
			this.servedaccount = com.ligadata.BaseTypes.StringImpl.Clone(other.servedaccount);
			this.servedofferings = com.ligadata.BaseTypes.StringImpl.Clone(other.servedofferings);
			this.terminationcause = com.ligadata.BaseTypes.StringImpl.Clone(other.terminationcause);
			this.chargingcontextid = com.ligadata.BaseTypes.StringImpl.Clone(other.chargingcontextid);
			this.servicecontextid = com.ligadata.BaseTypes.StringImpl.Clone(other.servicecontextid);
			this.servicesessionidcallreferencenumber = com.ligadata.BaseTypes.StringImpl.Clone(other.servicesessionidcallreferencenumber);
			this.sourcefilename = com.ligadata.BaseTypes.StringImpl.Clone(other.sourcefilename);
			this.networkcd = com.ligadata.BaseTypes.StringImpl.Clone(other.networkcd);
			this.serviceclassid = com.ligadata.BaseTypes.StringImpl.Clone(other.serviceclassid);
			this.paytype = com.ligadata.BaseTypes.StringImpl.Clone(other.paytype);
			this.callreferenceid = com.ligadata.BaseTypes.StringImpl.Clone(other.callreferenceid);
			this.mainamountused = com.ligadata.BaseTypes.StringImpl.Clone(other.mainamountused);
			this.serviceprovider_identifier = com.ligadata.BaseTypes.StringImpl.Clone(other.serviceprovider_identifier);
			this.vascode = com.ligadata.BaseTypes.StringImpl.Clone(other.vascode);
			this.originatingservices = com.ligadata.BaseTypes.StringImpl.Clone(other.originatingservices);
			this.originatingstation = com.ligadata.BaseTypes.StringImpl.Clone(other.originatingstation);
			this.terminatingstation = com.ligadata.BaseTypes.StringImpl.Clone(other.terminatingstation);
			this.subscriptiontype = com.ligadata.BaseTypes.StringImpl.Clone(other.subscriptiontype);
			this.transid = com.ligadata.BaseTypes.StringImpl.Clone(other.transid);
			this.channel = com.ligadata.BaseTypes.StringImpl.Clone(other.channel);
			this.calledcallingnumber = com.ligadata.BaseTypes.StringImpl.Clone(other.calledcallingnumber);
			this.eventtime = com.ligadata.BaseTypes.StringImpl.Clone(other.eventtime);
			this.category = com.ligadata.BaseTypes.StringImpl.Clone(other.category);
			this.sucategory = com.ligadata.BaseTypes.StringImpl.Clone(other.sucategory);
			this.content_name = com.ligadata.BaseTypes.StringImpl.Clone(other.content_name);
			this.terminating_number = com.ligadata.BaseTypes.StringImpl.Clone(other.terminating_number);
			this.compositerecords = com.ligadata.BaseTypes.StringImpl.Clone(other.compositerecords);
			this.file_name = com.ligadata.BaseTypes.StringImpl.Clone(other.file_name);
			this.file_offset = com.ligadata.BaseTypes.LongImpl.Clone(other.file_offset);
			this.kamanja_loaded_date = com.ligadata.BaseTypes.StringImpl.Clone(other.kamanja_loaded_date);
			this.file_mod_date = com.ligadata.BaseTypes.StringImpl.Clone(other.file_mod_date);
			this.bill_type_enrich = com.ligadata.BaseTypes.IntImpl.Clone(other.bill_type_enrich);
			this.network_type_enrich = com.ligadata.BaseTypes.IntImpl.Clone(other.network_type_enrich);
			this.data_type_enrich = com.ligadata.BaseTypes.IntImpl.Clone(other.data_type_enrich);
			this.tac_enrich = com.ligadata.BaseTypes.StringImpl.Clone(other.tac_enrich);
			this.normalizedchargedpartycgi_enrich = com.ligadata.BaseTypes.StringImpl.Clone(other.normalizedchargedpartycgi_enrich);
			this.latitude_enrich = com.ligadata.BaseTypes.DoubleImpl.Clone(other.latitude_enrich);
			this.longitude_enrich = com.ligadata.BaseTypes.DoubleImpl.Clone(other.longitude_enrich);
			this.locationtype_enrich = com.ligadata.BaseTypes.IntImpl.Clone(other.locationtype_enrich);
			this.date_key = com.ligadata.BaseTypes.IntImpl.Clone(other.date_key);
			this.msisdn_key = com.ligadata.BaseTypes.LongImpl.Clone(other.msisdn_key);
			this.event_timestamp_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.event_timestamp_enrich);
			this.original_timestamp_enrich = com.ligadata.BaseTypes.StringImpl.Clone(other.original_timestamp_enrich);
			this.is_free_enrich = com.ligadata.BaseTypes.IntImpl.Clone(other.is_free_enrich);
			this.data_volume_incoming_1 = com.ligadata.BaseTypes.LongImpl.Clone(other.data_volume_incoming_1);
			this.data_volume_outgoing_1 = com.ligadata.BaseTypes.LongImpl.Clone(other.data_volume_outgoing_1);
			this.ma_amount_used = com.ligadata.BaseTypes.DoubleImpl.Clone(other.ma_amount_used);
			this.imei_number = com.ligadata.BaseTypes.StringImpl.Clone(other.imei_number);
			this.imsi_number = com.ligadata.BaseTypes.StringImpl.Clone(other.imsi_number);
			this.msg_unique_id_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.msg_unique_id_enrich);
			this.base_file_name = com.ligadata.BaseTypes.StringImpl.Clone(other.base_file_name);
			this.path = com.ligadata.BaseTypes.StringImpl.Clone(other.path);
			this.line_number = com.ligadata.BaseTypes.LongImpl.Clone(other.line_number);
			this.file_id = com.ligadata.BaseTypes.StringImpl.Clone(other.file_id);
			this.processed_timestamp = com.ligadata.BaseTypes.LongImpl.Clone(other.processed_timestamp);
			this.ltz_event_timestamp_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.ltz_event_timestamp_enrich);
			this.transactionid_pp3 = com.ligadata.BaseTypes.StringImpl.Clone(other.transactionid_pp3);
			this.chargeamount_pp3 = com.ligadata.BaseTypes.DoubleImpl.Clone(other.chargeamount_pp3);
		 if (other.kamanja_system_null_flags != null ) { 
		 kamanja_system_null_flags = new scala.Array[Boolean](other.kamanja_system_null_flags.length); 
		 kamanja_system_null_flags = other.kamanja_system_null_flags.map(v => com.ligadata.BaseTypes.BoolImpl.Clone(v)); 
		 } 
		 else this.kamanja_system_null_flags = null; 

      this.setTimePartitionData(com.ligadata.BaseTypes.LongImpl.Clone(other.getTimePartitionData));
      return this;
    }
    

  override def hasUniqueKey(): Boolean = {
		return true;
	}  
    
  override def recomputeUniqueRowId(): Unit = {
    var initialseed: Long = 0xcbf29ce484222325L
    var curVal: Long = initialseed
    
    val uniqKeyColVals = Array[Any](switchcalltype,chrononumber,imsinumber,imeinumber,mobilenumber,calldate,chargingid,callduration,accesspointname,pdptype,pdpaddress,cellid,partialtypeindicator,callterminationcause,chargingcharacteristics,sgsnaddress,ggsnaddress,qosrequested1,qosused1,datavolumeoutgoing1,datavolumeincoming1,qosrequested2,qosused2,datavolumeoutgoing2,datavolumeincoming2,qosrequested3,qosused3,datavolumeoutgoing3,datavolumeincoming3,recordtype,amountofsdpbeforesession,amountonsdpaftersession,costofsession,dedicatedaccountvaluebeforecall,dedicatedaccountvalueaftercall,dedicatedaccountid,dedicatedamountused,numberofdedicatedaccountused,friendsandfamilyindicator,friendsandfamilynumber,ratedunitsfreeunits,ratedunitsdebit,ratedunitscredit,offerid,offertype,bonusamount,accountgroupid,pamserviceid,pamclassid,selectiontreetype,servedaccount,servedofferings,terminationcause,chargingcontextid,servicecontextid,servicesessionidcallreferencenumber,sourcefilename,networkcd,serviceclassid,paytype,callreferenceid,mainamountused,serviceprovider_identifier,vascode,originatingservices,originatingstation,terminatingstation,subscriptiontype,transid,channel,calledcallingnumber,eventtime,category,sucategory,content_name,terminating_number,compositerecords,transactionid_pp3,chargeamount_pp3)
    if(uniqKeyColVals != null && uniqKeyColVals.size > 0){
      uniqKeyColVals.foreach (ukV => {
        if (ukV.isInstanceOf[ContainerInterface]) {
          val byteArrays = ukV.asInstanceOf[ContainerInterface].getAllAttributesByteArrays()
          byteArrays.foreach(ba => {
            curVal = com.ligadata.Utils.ComputeHashKey.generateHashKey(curVal, ba)
          })
        } else {
          val byteArr = getKeyValueByteArray(ukV)
          curVal = com.ligadata.Utils.ComputeHashKey.generateHashKey(curVal, byteArr)
        }
      })
    }
     this.msg_unique_id_enrich = curVal;

  }

    override def computePartitionKeyHashIdValue(): Long = {
    var initialseed: Long = 0xcbf29ce484222325L
    var curVal: Long = initialseed
    
    val partitionKeys = Array[Any](msisdn_key)                
    if(partitionKeys != null && partitionKeys.size > 0){
      partitionKeys.foreach (pKV => {
         val byteArr = getKeyValueByteArray(pKV)
         curVal = com.ligadata.Utils.ComputeHashKey.generateHashKey(curVal, byteArr)
      })
    }
    return curVal;
  }

  override def getAllAttributesByteArrays(): Array[Array[Byte]] = {
    val outputArrayValues = scala.collection.mutable.ArrayBuffer[Array[Byte]]()
    val allAttributes = getOnlyValuesForAllAttributes()
    allAttributes.foreach (v => {
      if (v.isInstanceOf[ContainerInterface]) {
        outputArrayValues ++= v.asInstanceOf[ContainerInterface].getAllAttributesByteArrays()
      } else {
        outputArrayValues += getKeyValueByteArray(v)
      }
    })
    outputArrayValues.toArray
  }
 
  private val listSep = ",".getBytes

  private def getKeyValueByteArray(value: Any): Array[Byte] = {
    if (value == null) {
      return Array[Byte]()
    }
    // ???? is map & flatten is efficient than flatMap ?????
    if (value.isInstanceOf[List[_]]) {
      var firstVal = true
      return value.asInstanceOf[List[_]].flatMap(v => {
        if (firstVal) {
          firstVal = false
          getKeyValueByteArray(v)
        }
        else {
          listSep ++ getKeyValueByteArray(v)
        }
      }).toArray
    }
    if (value.isInstanceOf[Array[_]]) {
      var firstVal = true
      return value.asInstanceOf[Array[_]].flatMap(v => {
        if (firstVal) {
            firstVal = false
            getKeyValueByteArray(v)
          }
        else {
          listSep ++ getKeyValueByteArray(v)
        }
      })
    }

    value match {
      case b: Byte => Array[Byte](b)
      case bl: Boolean => if (bl) Array[Byte](1) else Array[Byte](0)
      case s: Short => ByteBuffer.allocate(2).putShort(s).array()
      case i: Int => ByteBuffer.allocate(4).putInt(i).array()
      case l: Long => ByteBuffer.allocate(8).putLong(l).array()
      case f: Float => ByteBuffer.allocate(4).putFloat(f).array()
      case d: Double => ByteBuffer.allocate(8).putDouble(d).array()
      case bi: BigInt => bi.toByteArray
      case x: String => x.getBytes
      // Handle other types here
    }
  }
 	 def withswitchcalltype(value: String) : CS5_CCN_GPRS_MA = {
		 this.switchcalltype = value 
		 return this 
 	 } 
	 def withchrononumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.chrononumber = value 
		 return this 
 	 } 
	 def withimsinumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.imsinumber = value 
		 return this 
 	 } 
	 def withimeinumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.imeinumber = value 
		 return this 
 	 } 
	 def withmobilenumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.mobilenumber = value 
		 return this 
 	 } 
	 def withcalldate(value: Long) : CS5_CCN_GPRS_MA = {
		 this.calldate = value 
		 return this 
 	 } 
	 def withchargingid(value: String) : CS5_CCN_GPRS_MA = {
		 this.chargingid = value 
		 return this 
 	 } 
	 def withcallduration(value: String) : CS5_CCN_GPRS_MA = {
		 this.callduration = value 
		 return this 
 	 } 
	 def withaccesspointname(value: String) : CS5_CCN_GPRS_MA = {
		 this.accesspointname = value 
		 return this 
 	 } 
	 def withpdptype(value: String) : CS5_CCN_GPRS_MA = {
		 this.pdptype = value 
		 return this 
 	 } 
	 def withpdpaddress(value: String) : CS5_CCN_GPRS_MA = {
		 this.pdpaddress = value 
		 return this 
 	 } 
	 def withcellid(value: String) : CS5_CCN_GPRS_MA = {
		 this.cellid = value 
		 return this 
 	 } 
	 def withpartialtypeindicator(value: String) : CS5_CCN_GPRS_MA = {
		 this.partialtypeindicator = value 
		 return this 
 	 } 
	 def withcallterminationcause(value: String) : CS5_CCN_GPRS_MA = {
		 this.callterminationcause = value 
		 return this 
 	 } 
	 def withchargingcharacteristics(value: String) : CS5_CCN_GPRS_MA = {
		 this.chargingcharacteristics = value 
		 return this 
 	 } 
	 def withsgsnaddress(value: String) : CS5_CCN_GPRS_MA = {
		 this.sgsnaddress = value 
		 return this 
 	 } 
	 def withggsnaddress(value: String) : CS5_CCN_GPRS_MA = {
		 this.ggsnaddress = value 
		 return this 
 	 } 
	 def withqosrequested1(value: String) : CS5_CCN_GPRS_MA = {
		 this.qosrequested1 = value 
		 return this 
 	 } 
	 def withqosused1(value: String) : CS5_CCN_GPRS_MA = {
		 this.qosused1 = value 
		 return this 
 	 } 
	 def withdatavolumeoutgoing1(value: String) : CS5_CCN_GPRS_MA = {
		 this.datavolumeoutgoing1 = value 
		 return this 
 	 } 
	 def withdatavolumeincoming1(value: String) : CS5_CCN_GPRS_MA = {
		 this.datavolumeincoming1 = value 
		 return this 
 	 } 
	 def withqosrequested2(value: String) : CS5_CCN_GPRS_MA = {
		 this.qosrequested2 = value 
		 return this 
 	 } 
	 def withqosused2(value: String) : CS5_CCN_GPRS_MA = {
		 this.qosused2 = value 
		 return this 
 	 } 
	 def withdatavolumeoutgoing2(value: String) : CS5_CCN_GPRS_MA = {
		 this.datavolumeoutgoing2 = value 
		 return this 
 	 } 
	 def withdatavolumeincoming2(value: String) : CS5_CCN_GPRS_MA = {
		 this.datavolumeincoming2 = value 
		 return this 
 	 } 
	 def withqosrequested3(value: String) : CS5_CCN_GPRS_MA = {
		 this.qosrequested3 = value 
		 return this 
 	 } 
	 def withqosused3(value: String) : CS5_CCN_GPRS_MA = {
		 this.qosused3 = value 
		 return this 
 	 } 
	 def withdatavolumeoutgoing3(value: String) : CS5_CCN_GPRS_MA = {
		 this.datavolumeoutgoing3 = value 
		 return this 
 	 } 
	 def withdatavolumeincoming3(value: String) : CS5_CCN_GPRS_MA = {
		 this.datavolumeincoming3 = value 
		 return this 
 	 } 
	 def withrecordtype(value: String) : CS5_CCN_GPRS_MA = {
		 this.recordtype = value 
		 return this 
 	 } 
	 def withamountofsdpbeforesession(value: String) : CS5_CCN_GPRS_MA = {
		 this.amountofsdpbeforesession = value 
		 return this 
 	 } 
	 def withamountonsdpaftersession(value: String) : CS5_CCN_GPRS_MA = {
		 this.amountonsdpaftersession = value 
		 return this 
 	 } 
	 def withcostofsession(value: String) : CS5_CCN_GPRS_MA = {
		 this.costofsession = value 
		 return this 
 	 } 
	 def withdedicatedaccountvaluebeforecall(value: String) : CS5_CCN_GPRS_MA = {
		 this.dedicatedaccountvaluebeforecall = value 
		 return this 
 	 } 
	 def withdedicatedaccountvalueaftercall(value: String) : CS5_CCN_GPRS_MA = {
		 this.dedicatedaccountvalueaftercall = value 
		 return this 
 	 } 
	 def withdedicatedaccountid(value: String) : CS5_CCN_GPRS_MA = {
		 this.dedicatedaccountid = value 
		 return this 
 	 } 
	 def withdedicatedamountused(value: String) : CS5_CCN_GPRS_MA = {
		 this.dedicatedamountused = value 
		 return this 
 	 } 
	 def withnumberofdedicatedaccountused(value: String) : CS5_CCN_GPRS_MA = {
		 this.numberofdedicatedaccountused = value 
		 return this 
 	 } 
	 def withfriendsandfamilyindicator(value: String) : CS5_CCN_GPRS_MA = {
		 this.friendsandfamilyindicator = value 
		 return this 
 	 } 
	 def withfriendsandfamilynumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.friendsandfamilynumber = value 
		 return this 
 	 } 
	 def withratedunitsfreeunits(value: String) : CS5_CCN_GPRS_MA = {
		 this.ratedunitsfreeunits = value 
		 return this 
 	 } 
	 def withratedunitsdebit(value: String) : CS5_CCN_GPRS_MA = {
		 this.ratedunitsdebit = value 
		 return this 
 	 } 
	 def withratedunitscredit(value: String) : CS5_CCN_GPRS_MA = {
		 this.ratedunitscredit = value 
		 return this 
 	 } 
	 def withofferid(value: String) : CS5_CCN_GPRS_MA = {
		 this.offerid = value 
		 return this 
 	 } 
	 def withoffertype(value: String) : CS5_CCN_GPRS_MA = {
		 this.offertype = value 
		 return this 
 	 } 
	 def withbonusamount(value: String) : CS5_CCN_GPRS_MA = {
		 this.bonusamount = value 
		 return this 
 	 } 
	 def withaccountgroupid(value: String) : CS5_CCN_GPRS_MA = {
		 this.accountgroupid = value 
		 return this 
 	 } 
	 def withpamserviceid(value: String) : CS5_CCN_GPRS_MA = {
		 this.pamserviceid = value 
		 return this 
 	 } 
	 def withpamclassid(value: String) : CS5_CCN_GPRS_MA = {
		 this.pamclassid = value 
		 return this 
 	 } 
	 def withselectiontreetype(value: String) : CS5_CCN_GPRS_MA = {
		 this.selectiontreetype = value 
		 return this 
 	 } 
	 def withservedaccount(value: String) : CS5_CCN_GPRS_MA = {
		 this.servedaccount = value 
		 return this 
 	 } 
	 def withservedofferings(value: String) : CS5_CCN_GPRS_MA = {
		 this.servedofferings = value 
		 return this 
 	 } 
	 def withterminationcause(value: String) : CS5_CCN_GPRS_MA = {
		 this.terminationcause = value 
		 return this 
 	 } 
	 def withchargingcontextid(value: String) : CS5_CCN_GPRS_MA = {
		 this.chargingcontextid = value 
		 return this 
 	 } 
	 def withservicecontextid(value: String) : CS5_CCN_GPRS_MA = {
		 this.servicecontextid = value 
		 return this 
 	 } 
	 def withservicesessionidcallreferencenumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.servicesessionidcallreferencenumber = value 
		 return this 
 	 } 
	 def withsourcefilename(value: String) : CS5_CCN_GPRS_MA = {
		 this.sourcefilename = value 
		 return this 
 	 } 
	 def withnetworkcd(value: String) : CS5_CCN_GPRS_MA = {
		 this.networkcd = value 
		 return this 
 	 } 
	 def withserviceclassid(value: String) : CS5_CCN_GPRS_MA = {
		 this.serviceclassid = value 
		 return this 
 	 } 
	 def withpaytype(value: String) : CS5_CCN_GPRS_MA = {
		 this.paytype = value 
		 return this 
 	 } 
	 def withcallreferenceid(value: String) : CS5_CCN_GPRS_MA = {
		 this.callreferenceid = value 
		 return this 
 	 } 
	 def withmainamountused(value: String) : CS5_CCN_GPRS_MA = {
		 this.mainamountused = value 
		 return this 
 	 } 
	 def withserviceprovider_identifier(value: String) : CS5_CCN_GPRS_MA = {
		 this.serviceprovider_identifier = value 
		 return this 
 	 } 
	 def withvascode(value: String) : CS5_CCN_GPRS_MA = {
		 this.vascode = value 
		 return this 
 	 } 
	 def withoriginatingservices(value: String) : CS5_CCN_GPRS_MA = {
		 this.originatingservices = value 
		 return this 
 	 } 
	 def withoriginatingstation(value: String) : CS5_CCN_GPRS_MA = {
		 this.originatingstation = value 
		 return this 
 	 } 
	 def withterminatingstation(value: String) : CS5_CCN_GPRS_MA = {
		 this.terminatingstation = value 
		 return this 
 	 } 
	 def withsubscriptiontype(value: String) : CS5_CCN_GPRS_MA = {
		 this.subscriptiontype = value 
		 return this 
 	 } 
	 def withtransid(value: String) : CS5_CCN_GPRS_MA = {
		 this.transid = value 
		 return this 
 	 } 
	 def withchannel(value: String) : CS5_CCN_GPRS_MA = {
		 this.channel = value 
		 return this 
 	 } 
	 def withcalledcallingnumber(value: String) : CS5_CCN_GPRS_MA = {
		 this.calledcallingnumber = value 
		 return this 
 	 } 
	 def witheventtime(value: String) : CS5_CCN_GPRS_MA = {
		 this.eventtime = value 
		 return this 
 	 } 
	 def withcategory(value: String) : CS5_CCN_GPRS_MA = {
		 this.category = value 
		 return this 
 	 } 
	 def withsucategory(value: String) : CS5_CCN_GPRS_MA = {
		 this.sucategory = value 
		 return this 
 	 } 
	 def withcontent_name(value: String) : CS5_CCN_GPRS_MA = {
		 this.content_name = value 
		 return this 
 	 } 
	 def withterminating_number(value: String) : CS5_CCN_GPRS_MA = {
		 this.terminating_number = value 
		 return this 
 	 } 
	 def withcompositerecords(value: String) : CS5_CCN_GPRS_MA = {
		 this.compositerecords = value 
		 return this 
 	 } 
	 def withfile_name(value: String) : CS5_CCN_GPRS_MA = {
		 this.file_name = value 
		 return this 
 	 } 
	 def withfile_offset(value: Long) : CS5_CCN_GPRS_MA = {
		 this.file_offset = value 
		 return this 
 	 } 
	 def withkamanja_loaded_date(value: String) : CS5_CCN_GPRS_MA = {
		 this.kamanja_loaded_date = value 
		 return this 
 	 } 
	 def withfile_mod_date(value: String) : CS5_CCN_GPRS_MA = {
		 this.file_mod_date = value 
		 return this 
 	 } 
	 def withbill_type_enrich(value: Int) : CS5_CCN_GPRS_MA = {
		 this.bill_type_enrich = value 
		 return this 
 	 } 
	 def withnetwork_type_enrich(value: Int) : CS5_CCN_GPRS_MA = {
		 this.network_type_enrich = value 
		 return this 
 	 } 
	 def withdata_type_enrich(value: Int) : CS5_CCN_GPRS_MA = {
		 this.data_type_enrich = value 
		 return this 
 	 } 
	 def withtac_enrich(value: String) : CS5_CCN_GPRS_MA = {
		 this.tac_enrich = value 
		 return this 
 	 } 
	 def withnormalizedchargedpartycgi_enrich(value: String) : CS5_CCN_GPRS_MA = {
		 this.normalizedchargedpartycgi_enrich = value 
		 return this 
 	 } 
	 def withlatitude_enrich(value: Double) : CS5_CCN_GPRS_MA = {
		 this.latitude_enrich = value 
		 return this 
 	 } 
	 def withlongitude_enrich(value: Double) : CS5_CCN_GPRS_MA = {
		 this.longitude_enrich = value 
		 return this 
 	 } 
	 def withlocationtype_enrich(value: Int) : CS5_CCN_GPRS_MA = {
		 this.locationtype_enrich = value 
		 return this 
 	 } 
	 def withdate_key(value: Int) : CS5_CCN_GPRS_MA = {
		 this.date_key = value 
		 return this 
 	 } 
	 def withmsisdn_key(value: Long) : CS5_CCN_GPRS_MA = {
		 this.msisdn_key = value 
		 return this 
 	 } 
	 def withevent_timestamp_enrich(value: Long) : CS5_CCN_GPRS_MA = {
		 this.event_timestamp_enrich = value 
		 return this 
 	 } 
	 def withoriginal_timestamp_enrich(value: String) : CS5_CCN_GPRS_MA = {
		 this.original_timestamp_enrich = value 
		 return this 
 	 } 
	 def withis_free_enrich(value: Int) : CS5_CCN_GPRS_MA = {
		 this.is_free_enrich = value 
		 return this 
 	 } 
	 def withdata_volume_incoming_1(value: Long) : CS5_CCN_GPRS_MA = {
		 this.data_volume_incoming_1 = value 
		 return this 
 	 } 
	 def withdata_volume_outgoing_1(value: Long) : CS5_CCN_GPRS_MA = {
		 this.data_volume_outgoing_1 = value 
		 return this 
 	 } 
	 def withma_amount_used(value: Double) : CS5_CCN_GPRS_MA = {
		 this.ma_amount_used = value 
		 return this 
 	 } 
	 def withimei_number(value: String) : CS5_CCN_GPRS_MA = {
		 this.imei_number = value 
		 return this 
 	 } 
	 def withimsi_number(value: String) : CS5_CCN_GPRS_MA = {
		 this.imsi_number = value 
		 return this 
 	 } 
	 def withmsg_unique_id_enrich(value: Long) : CS5_CCN_GPRS_MA = {
		 this.msg_unique_id_enrich = value 
		 return this 
 	 } 
	 def withbase_file_name(value: String) : CS5_CCN_GPRS_MA = {
		 this.base_file_name = value 
		 return this 
 	 } 
	 def withpath(value: String) : CS5_CCN_GPRS_MA = {
		 this.path = value 
		 return this 
 	 } 
	 def withline_number(value: Long) : CS5_CCN_GPRS_MA = {
		 this.line_number = value 
		 return this 
 	 } 
	 def withfile_id(value: String) : CS5_CCN_GPRS_MA = {
		 this.file_id = value 
		 return this 
 	 } 
	 def withprocessed_timestamp(value: Long) : CS5_CCN_GPRS_MA = {
		 this.processed_timestamp = value 
		 return this 
 	 } 
	 def withltz_event_timestamp_enrich(value: Long) : CS5_CCN_GPRS_MA = {
		 this.ltz_event_timestamp_enrich = value 
		 return this 
 	 } 
	 def withtransactionid_pp3(value: String) : CS5_CCN_GPRS_MA = {
		 this.transactionid_pp3 = value 
		 return this 
 	 } 
	 def withchargeamount_pp3(value: Double) : CS5_CCN_GPRS_MA = {
		 this.chargeamount_pp3 = value 
		 return this 
 	 } 
	 def withkamanja_system_null_flags(value: scala.Array[Boolean]) : CS5_CCN_GPRS_MA = {
		 this.kamanja_system_null_flags = value 
		 return this 
 	 } 



    override def getRDDObject(): RDDObject[ContainerInterface] = CS5_CCN_GPRS_MA.asInstanceOf[RDDObject[ContainerInterface]];


        def isCaseSensitive(): Boolean = CS5_CCN_GPRS_MA.isCaseSensitive(); 
    def caseSensitiveKey(keyName: String): String = {
      if(isCaseSensitive)
        return keyName;
      else return keyName.toLowerCase;
    }


    
    def this(factory:MessageFactoryInterface) = {
      this(factory, null)
     }
    
    def this(other: CS5_CCN_GPRS_MA) = {
      this(other.getFactory.asInstanceOf[MessageFactoryInterface], other)
    }

}
