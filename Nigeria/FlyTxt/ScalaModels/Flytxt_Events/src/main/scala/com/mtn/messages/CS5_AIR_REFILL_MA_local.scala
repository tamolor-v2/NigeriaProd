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
    
 
object CS5_AIR_REFILL_MA extends RDDObject[CS5_AIR_REFILL_MA] with MessageFactoryInterface { 
 
  val log = LogManager.getLogger(getClass)
	type T = CS5_AIR_REFILL_MA ;
	override def getFullTypeName: String = "com.mtn.messages.CS5_AIR_REFILL_MA"; 
	override def getTypeNameSpace: String = "com.mtn.messages"; 
	override def getTypeName: String = "CS5_AIR_REFILL_MA"; 
	override def getTypeVersion: String = "000000.000001.000000"; 
	override def getSchemaId: Int = 2001213; 
	private var elementId: Long = 0L; 
	override def setElementId(elemId: Long): Unit = { elementId = elemId; } 
	override def getElementId: Long = elementId; 
	override def getTenantId: String = "flare"; 
	override def createInstance: CS5_AIR_REFILL_MA = new CS5_AIR_REFILL_MA(CS5_AIR_REFILL_MA); 
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
  
    override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.messages" , "name" : "cs5_air_refill_ma" , "fields":[{ "name" : "edrtype" , "type" : "string"},{ "name" : "originnodetype" , "type" : "string"},{ "name" : "originhostname" , "type" : "string"},{ "name" : "originfileid" , "type" : "string"},{ "name" : "origintransactionid" , "type" : "string"},{ "name" : "originoperatorid" , "type" : "string"},{ "name" : "origintimestamp" , "type" : "string"},{ "name" : "hostname" , "type" : "string"},{ "name" : "localsequencenumber" , "type" : "int"},{ "name" : "timestamp_v" , "type" : "long"},{ "name" : "currentserviceclass" , "type" : "int"},{ "name" : "voucherbasedrefill" , "type" : "string"},{ "name" : "transactiontype" , "type" : "string"},{ "name" : "transactioncode" , "type" : "string"},{ "name" : "transactionamount" , "type" : "string"},{ "name" : "transactioncurrency" , "type" : "string"},{ "name" : "refillamountconverted" , "type" : "string"},{ "name" : "refilldivisionamount" , "type" : "string"},{ "name" : "refilltype" , "type" : "int"},{ "name" : "refillprofileid" , "type" : "string"},{ "name" : "segmentationid" , "type" : "string"},{ "name" : "voucherserialnumber" , "type" : "string"},{ "name" : "vouchergroupid" , "type" : "string"},{ "name" : "accountnumber" , "type" : "string"},{ "name" : "accountcurrency" , "type" : "string"},{ "name" : "subscribernumber" , "type" : "string"},{ "name" : "promotionannouncementcode" , "type" : "int"},{ "name" : "accountflags" , "type" : "string"},{ "name" : "accountbalance" , "type" : "string"},{ "name" : "accumulatedrefillvalue" , "type" : "string"},{ "name" : "accumulatedrefillcounter" , "type" : "int"},{ "name" : "accumulatedprogressionvalue" , "type" : "string"},{ "name" : "accumulatedprogressioncounter" , "type" : "int"},{ "name" : "creditclearanceperiod" , "type" : "int"},{ "name" : "promotionplan" , "type" : "string"},{ "name" : "permanentserviceclass" , "type" : "int"},{ "name" : "temporaryserviceclass" , "type" : "int"},{ "name" : "temporaryserviceclassexpirydate" , "type" : "string"},{ "name" : "refilloption" , "type" : "int"},{ "name" : "servicefeeexpirydate" , "type" : "string"},{ "name" : "serviceremovalgraceperiod" , "type" : "int"},{ "name" : "serviceoffering" , "type" : "int"},{ "name" : "supervisionexpirydate" , "type" : "string"},{ "name" : "communityid1" , "type" : "int"},{ "name" : "communityid2" , "type" : "int"},{ "name" : "communityid3" , "type" : "int"},{ "name" : "offeridentifier" , "type" : "int"},{ "name" : "offerstartdate" , "type" : "string"},{ "name" : "offerexpirydate" , "type" : "string"},{ "name" : "offertype" , "type" : "string"},{ "name" : "offeridentifier____1" , "type" : "int"},{ "name" : "offerstartdate____1" , "type" : "string"},{ "name" : "offerexpirydate____1" , "type" : "string"},{ "name" : "offertype____1" , "type" : "string"},{ "name" : "offeridentifier____2" , "type" : "int"},{ "name" : "offerstartdate____2" , "type" : "string"},{ "name" : "offerexpirydate____2" , "type" : "string"},{ "name" : "offertype____2" , "type" : "string"},{ "name" : "offeridentifier____3" , "type" : "int"},{ "name" : "offerstartdate____3" , "type" : "string"},{ "name" : "offerexpirydate____3" , "type" : "string"},{ "name" : "offertype____3" , "type" : "string"},{ "name" : "offeridentifier____4" , "type" : "int"},{ "name" : "offerstartdate____4" , "type" : "string"},{ "name" : "offerexpirydate____4" , "type" : "string"},{ "name" : "offertype____4" , "type" : "string"},{ "name" : "accountflags____1" , "type" : "string"},{ "name" : "accountbalance____1" , "type" : "string"},{ "name" : "accumulatedrefillvalue____1" , "type" : "string"},{ "name" : "accumulatedrefillcounter____1" , "type" : "int"},{ "name" : "accumulatedprogressionvalue____1" , "type" : "string"},{ "name" : "accumulatedprogressioncounter____1" , "type" : "int"},{ "name" : "creditclearanceperiod____1" , "type" : "int"},{ "name" : "promotionplan____1" , "type" : "string"},{ "name" : "permanentserviceclass____1" , "type" : "int"},{ "name" : "temporaryserviceclass____1" , "type" : "int"},{ "name" : "temporaryserviceclassexpirydate____1" , "type" : "string"},{ "name" : "refilloption____1" , "type" : "int"},{ "name" : "servicefeeexpirydate____1" , "type" : "string"},{ "name" : "serviceremovalgraceperiod____1" , "type" : "int"},{ "name" : "serviceoffering____1" , "type" : "int"},{ "name" : "supervisionexpirydate____1" , "type" : "string"},{ "name" : "communityid1____1" , "type" : "int"},{ "name" : "communityid2____1" , "type" : "int"},{ "name" : "communityid3____1" , "type" : "int"},{ "name" : "offeridentifier____5" , "type" : "int"},{ "name" : "offerstartdate____5" , "type" : "string"},{ "name" : "offerexpirydate____5" , "type" : "string"},{ "name" : "offertype____5" , "type" : "string"},{ "name" : "offeridentifier____6" , "type" : "int"},{ "name" : "offerstartdate____6" , "type" : "string"},{ "name" : "offerexpirydate____6" , "type" : "string"},{ "name" : "offertype____6" , "type" : "string"},{ "name" : "offeridentifier____7" , "type" : "int"},{ "name" : "offerstartdate____7" , "type" : "string"},{ "name" : "offerexpirydate____7" , "type" : "string"},{ "name" : "offertype____7" , "type" : "string"},{ "name" : "offeridentifier____8" , "type" : "int"},{ "name" : "offerstartdate____8" , "type" : "string"},{ "name" : "offerexpirydate____8" , "type" : "string"},{ "name" : "offertype____8" , "type" : "string"},{ "name" : "offeridentifier____9" , "type" : "int"},{ "name" : "offerstartdate____9" , "type" : "string"},{ "name" : "offerexpirydate____9" , "type" : "string"},{ "name" : "offertype____9" , "type" : "string"},{ "name" : "refillpromodivisionamount" , "type" : "string"},{ "name" : "supervisiondayspromopart" , "type" : "int"},{ "name" : "supervisiondayssurplus" , "type" : "int"},{ "name" : "servicefeedayspromopart" , "type" : "int"},{ "name" : "servicefeedayssurplus" , "type" : "int"},{ "name" : "maximumservicefeeperiod" , "type" : "int"},{ "name" : "maximumsupervisionperiod" , "type" : "int"},{ "name" : "activationdate" , "type" : "string"},{ "name" : "welcomestatus" , "type" : "string"},{ "name" : "voucheragent" , "type" : "string"},{ "name" : "promotionplanallocstartdate" , "type" : "string"},{ "name" : "accountgroupid" , "type" : "int"},{ "name" : "externaldata1" , "type" : "string"},{ "name" : "externaldata2" , "type" : "string"},{ "name" : "externaldata3" , "type" : "string"},{ "name" : "externaldata4" , "type" : "string"},{ "name" : "locationnumber" , "type" : "string"},{ "name" : "voucheractivationcode" , "type" : "string"},{ "name" : "accountcurrencycleared" , "type" : "string"},{ "name" : "ignoreserviceclasshierarchy" , "type" : "string"},{ "name" : "selectiontreeid" , "type" : "string"},{ "name" : "selectiontreeversion" , "type" : "string"},{ "name" : "selectiontreeid____1" , "type" : "string"},{ "name" : "selectiontreeversion____1" , "type" : "string"},{ "name" : "selectiontreeid____2" , "type" : "string"},{ "name" : "selectiontreeversion____2" , "type" : "string"},{ "name" : "selectiontreeid____3" , "type" : "string"},{ "name" : "selectiontreeversion____3" , "type" : "string"},{ "name" : "selectiontreeid____4" , "type" : "string"},{ "name" : "selectiontreeversion____4" , "type" : "string"},{ "name" : "parameterid" , "type" : "string"},{ "name" : "parametervalue" , "type" : "string"},{ "name" : "parameterid____1" , "type" : "string"},{ "name" : "parametervalue____1" , "type" : "string"},{ "name" : "parameterid____2" , "type" : "string"},{ "name" : "parametervalue____2" , "type" : "string"},{ "name" : "parameterid____3" , "type" : "string"},{ "name" : "parametervalue____3" , "type" : "string"},{ "name" : "parameterid____4" , "type" : "string"},{ "name" : "parametervalue____4" , "type" : "string"},{ "name" : "accounthomeregion" , "type" : "int"},{ "name" : "subscriberregion" , "type" : "int"},{ "name" : "voucherregion" , "type" : "int"},{ "name" : "promotionplanallocenddate" , "type" : "string"},{ "name" : "requestedrefilltype" , "type" : "int"},{ "name" : "cellglobalid" , "type" : "string"},{ "name" : "dasizeafter" , "type" : "string"},{ "name" : "dasizebefore" , "type" : "string"},{ "name" : "accsizebefore" , "type" : "string"},{ "name" : "accsizeafter" , "type" : "string"},{ "name" : "serviceclassid" , "type" : "string"},{ "name" : "paytype" , "type" : "string"},{ "name" : "callreferenceid" , "type" : "string"},{ "name" : "mainamountused" , "type" : "string"},{ "name" : "file_name" , "type" : "string"},{ "name" : "file_offset" , "type" : "long"},{ "name" : "kamanja_loaded_date" , "type" : "string"},{ "name" : "file_mod_date" , "type" : "string"},{ "name" : "recharge_type_enrich" , "type" : "int"},{ "name" : "date_key" , "type" : "int"},{ "name" : "msisdn_key" , "type" : "long"},{ "name" : "event_timestamp_enrich" , "type" : "long"},{ "name" : "original_timestamp_enrich" , "type" : "string"},{ "name" : "transaction_amt" , "type" : "double"},{ "name" : "origin_time_stamp" , "type" : "string"},{ "name" : "voucher_serial_nr" , "type" : "string"},{ "name" : "origin_host_nm" , "type" : "string"},{ "name" : "external_data1" , "type" : "string"},{ "name" : "msg_unique_id_enrich" , "type" : "long"},{ "name" : "base_file_name" , "type" : "string"},{ "name" : "path" , "type" : "string"},{ "name" : "line_number" , "type" : "long"},{ "name" : "file_id" , "type" : "string"},{ "name" : "processed_timestamp" , "type" : "long"},{ "name" : "ltz_event_timestamp_enrich" , "type" : "long"},{ "name" : "kamanja_system_null_flags" , "type" :  {"type" : "array", "items" : "boolean"}}]}""";  
  
      var attributeTypes = generateAttributeTypes;
    
    private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
      var attributeTypes = new Array[AttributeTypeInfo](181);
                                                                        		 attributeTypes(0) = new AttributeTypeInfo("edrtype", 0, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(1) = new AttributeTypeInfo("originnodetype", 1, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(2) = new AttributeTypeInfo("originhostname", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(3) = new AttributeTypeInfo("originfileid", 3, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(4) = new AttributeTypeInfo("origintransactionid", 4, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(5) = new AttributeTypeInfo("originoperatorid", 5, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(6) = new AttributeTypeInfo("origintimestamp", 6, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(7) = new AttributeTypeInfo("hostname", 7, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(8) = new AttributeTypeInfo("localsequencenumber", 8, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(9) = new AttributeTypeInfo("timestamp_v", 9, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(10) = new AttributeTypeInfo("currentserviceclass", 10, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(11) = new AttributeTypeInfo("voucherbasedrefill", 11, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(12) = new AttributeTypeInfo("transactiontype", 12, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(13) = new AttributeTypeInfo("transactioncode", 13, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(14) = new AttributeTypeInfo("transactionamount", 14, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(15) = new AttributeTypeInfo("transactioncurrency", 15, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(16) = new AttributeTypeInfo("refillamountconverted", 16, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(17) = new AttributeTypeInfo("refilldivisionamount", 17, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(18) = new AttributeTypeInfo("refilltype", 18, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(19) = new AttributeTypeInfo("refillprofileid", 19, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(20) = new AttributeTypeInfo("segmentationid", 20, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(21) = new AttributeTypeInfo("voucherserialnumber", 21, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(22) = new AttributeTypeInfo("vouchergroupid", 22, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(23) = new AttributeTypeInfo("accountnumber", 23, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(24) = new AttributeTypeInfo("accountcurrency", 24, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(25) = new AttributeTypeInfo("subscribernumber", 25, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(26) = new AttributeTypeInfo("promotionannouncementcode", 26, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(27) = new AttributeTypeInfo("accountflags", 27, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(28) = new AttributeTypeInfo("accountbalance", 28, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(29) = new AttributeTypeInfo("accumulatedrefillvalue", 29, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(30) = new AttributeTypeInfo("accumulatedrefillcounter", 30, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(31) = new AttributeTypeInfo("accumulatedprogressionvalue", 31, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(32) = new AttributeTypeInfo("accumulatedprogressioncounter", 32, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(33) = new AttributeTypeInfo("creditclearanceperiod", 33, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(34) = new AttributeTypeInfo("promotionplan", 34, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(35) = new AttributeTypeInfo("permanentserviceclass", 35, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(36) = new AttributeTypeInfo("temporaryserviceclass", 36, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(37) = new AttributeTypeInfo("temporaryserviceclassexpirydate", 37, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(38) = new AttributeTypeInfo("refilloption", 38, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(39) = new AttributeTypeInfo("servicefeeexpirydate", 39, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(40) = new AttributeTypeInfo("serviceremovalgraceperiod", 40, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(41) = new AttributeTypeInfo("serviceoffering", 41, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(42) = new AttributeTypeInfo("supervisionexpirydate", 42, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(43) = new AttributeTypeInfo("communityid1", 43, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(44) = new AttributeTypeInfo("communityid2", 44, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(45) = new AttributeTypeInfo("communityid3", 45, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(46) = new AttributeTypeInfo("offeridentifier", 46, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(47) = new AttributeTypeInfo("offerstartdate", 47, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(48) = new AttributeTypeInfo("offerexpirydate", 48, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(49) = new AttributeTypeInfo("offertype", 49, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(50) = new AttributeTypeInfo("offeridentifier____1", 50, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(51) = new AttributeTypeInfo("offerstartdate____1", 51, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(52) = new AttributeTypeInfo("offerexpirydate____1", 52, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(53) = new AttributeTypeInfo("offertype____1", 53, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(54) = new AttributeTypeInfo("offeridentifier____2", 54, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(55) = new AttributeTypeInfo("offerstartdate____2", 55, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(56) = new AttributeTypeInfo("offerexpirydate____2", 56, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(57) = new AttributeTypeInfo("offertype____2", 57, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(58) = new AttributeTypeInfo("offeridentifier____3", 58, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(59) = new AttributeTypeInfo("offerstartdate____3", 59, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(60) = new AttributeTypeInfo("offerexpirydate____3", 60, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(61) = new AttributeTypeInfo("offertype____3", 61, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(62) = new AttributeTypeInfo("offeridentifier____4", 62, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(63) = new AttributeTypeInfo("offerstartdate____4", 63, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(64) = new AttributeTypeInfo("offerexpirydate____4", 64, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(65) = new AttributeTypeInfo("offertype____4", 65, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(66) = new AttributeTypeInfo("accountflags____1", 66, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(67) = new AttributeTypeInfo("accountbalance____1", 67, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(68) = new AttributeTypeInfo("accumulatedrefillvalue____1", 68, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(69) = new AttributeTypeInfo("accumulatedrefillcounter____1", 69, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(70) = new AttributeTypeInfo("accumulatedprogressionvalue____1", 70, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(71) = new AttributeTypeInfo("accumulatedprogressioncounter____1", 71, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(72) = new AttributeTypeInfo("creditclearanceperiod____1", 72, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(73) = new AttributeTypeInfo("promotionplan____1", 73, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(74) = new AttributeTypeInfo("permanentserviceclass____1", 74, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(75) = new AttributeTypeInfo("temporaryserviceclass____1", 75, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(76) = new AttributeTypeInfo("temporaryserviceclassexpirydate____1", 76, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(77) = new AttributeTypeInfo("refilloption____1", 77, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(78) = new AttributeTypeInfo("servicefeeexpirydate____1", 78, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(79) = new AttributeTypeInfo("serviceremovalgraceperiod____1", 79, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(80) = new AttributeTypeInfo("serviceoffering____1", 80, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(81) = new AttributeTypeInfo("supervisionexpirydate____1", 81, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(82) = new AttributeTypeInfo("communityid1____1", 82, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(83) = new AttributeTypeInfo("communityid2____1", 83, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(84) = new AttributeTypeInfo("communityid3____1", 84, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(85) = new AttributeTypeInfo("offeridentifier____5", 85, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(86) = new AttributeTypeInfo("offerstartdate____5", 86, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(87) = new AttributeTypeInfo("offerexpirydate____5", 87, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(88) = new AttributeTypeInfo("offertype____5", 88, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(89) = new AttributeTypeInfo("offeridentifier____6", 89, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(90) = new AttributeTypeInfo("offerstartdate____6", 90, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(91) = new AttributeTypeInfo("offerexpirydate____6", 91, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(92) = new AttributeTypeInfo("offertype____6", 92, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(93) = new AttributeTypeInfo("offeridentifier____7", 93, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(94) = new AttributeTypeInfo("offerstartdate____7", 94, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(95) = new AttributeTypeInfo("offerexpirydate____7", 95, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(96) = new AttributeTypeInfo("offertype____7", 96, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(97) = new AttributeTypeInfo("offeridentifier____8", 97, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(98) = new AttributeTypeInfo("offerstartdate____8", 98, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(99) = new AttributeTypeInfo("offerexpirydate____8", 99, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(100) = new AttributeTypeInfo("offertype____8", 100, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(101) = new AttributeTypeInfo("offeridentifier____9", 101, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(102) = new AttributeTypeInfo("offerstartdate____9", 102, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(103) = new AttributeTypeInfo("offerexpirydate____9", 103, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(104) = new AttributeTypeInfo("offertype____9", 104, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(105) = new AttributeTypeInfo("refillpromodivisionamount", 105, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(106) = new AttributeTypeInfo("supervisiondayspromopart", 106, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(107) = new AttributeTypeInfo("supervisiondayssurplus", 107, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(108) = new AttributeTypeInfo("servicefeedayspromopart", 108, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(109) = new AttributeTypeInfo("servicefeedayssurplus", 109, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(110) = new AttributeTypeInfo("maximumservicefeeperiod", 110, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(111) = new AttributeTypeInfo("maximumsupervisionperiod", 111, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(112) = new AttributeTypeInfo("activationdate", 112, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(113) = new AttributeTypeInfo("welcomestatus", 113, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(114) = new AttributeTypeInfo("voucheragent", 114, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(115) = new AttributeTypeInfo("promotionplanallocstartdate", 115, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(116) = new AttributeTypeInfo("accountgroupid", 116, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(117) = new AttributeTypeInfo("externaldata1", 117, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(118) = new AttributeTypeInfo("externaldata2", 118, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(119) = new AttributeTypeInfo("externaldata3", 119, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(120) = new AttributeTypeInfo("externaldata4", 120, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(121) = new AttributeTypeInfo("locationnumber", 121, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(122) = new AttributeTypeInfo("voucheractivationcode", 122, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(123) = new AttributeTypeInfo("accountcurrencycleared", 123, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(124) = new AttributeTypeInfo("ignoreserviceclasshierarchy", 124, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(125) = new AttributeTypeInfo("selectiontreeid", 125, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(126) = new AttributeTypeInfo("selectiontreeversion", 126, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(127) = new AttributeTypeInfo("selectiontreeid____1", 127, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(128) = new AttributeTypeInfo("selectiontreeversion____1", 128, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(129) = new AttributeTypeInfo("selectiontreeid____2", 129, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(130) = new AttributeTypeInfo("selectiontreeversion____2", 130, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(131) = new AttributeTypeInfo("selectiontreeid____3", 131, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(132) = new AttributeTypeInfo("selectiontreeversion____3", 132, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(133) = new AttributeTypeInfo("selectiontreeid____4", 133, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(134) = new AttributeTypeInfo("selectiontreeversion____4", 134, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(135) = new AttributeTypeInfo("parameterid", 135, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(136) = new AttributeTypeInfo("parametervalue", 136, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(137) = new AttributeTypeInfo("parameterid____1", 137, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(138) = new AttributeTypeInfo("parametervalue____1", 138, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(139) = new AttributeTypeInfo("parameterid____2", 139, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(140) = new AttributeTypeInfo("parametervalue____2", 140, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(141) = new AttributeTypeInfo("parameterid____3", 141, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(142) = new AttributeTypeInfo("parametervalue____3", 142, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(143) = new AttributeTypeInfo("parameterid____4", 143, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(144) = new AttributeTypeInfo("parametervalue____4", 144, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(145) = new AttributeTypeInfo("accounthomeregion", 145, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(146) = new AttributeTypeInfo("subscriberregion", 146, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(147) = new AttributeTypeInfo("voucherregion", 147, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(148) = new AttributeTypeInfo("promotionplanallocenddate", 148, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(149) = new AttributeTypeInfo("requestedrefilltype", 149, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(150) = new AttributeTypeInfo("cellglobalid", 150, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(151) = new AttributeTypeInfo("dasizeafter", 151, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(152) = new AttributeTypeInfo("dasizebefore", 152, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(153) = new AttributeTypeInfo("accsizebefore", 153, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(154) = new AttributeTypeInfo("accsizeafter", 154, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(155) = new AttributeTypeInfo("serviceclassid", 155, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(156) = new AttributeTypeInfo("paytype", 156, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(157) = new AttributeTypeInfo("callreferenceid", 157, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(158) = new AttributeTypeInfo("mainamountused", 158, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(159) = new AttributeTypeInfo("file_name", 159, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(160) = new AttributeTypeInfo("file_offset", 160, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(161) = new AttributeTypeInfo("kamanja_loaded_date", 161, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(162) = new AttributeTypeInfo("file_mod_date", 162, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(163) = new AttributeTypeInfo("recharge_type_enrich", 163, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(164) = new AttributeTypeInfo("date_key", 164, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(165) = new AttributeTypeInfo("msisdn_key", 165, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(166) = new AttributeTypeInfo("event_timestamp_enrich", 166, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(167) = new AttributeTypeInfo("original_timestamp_enrich", 167, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(168) = new AttributeTypeInfo("transaction_amt", 168, AttributeTypeInfo.TypeCategory.DOUBLE, -1, -1, 0)
		 attributeTypes(169) = new AttributeTypeInfo("origin_time_stamp", 169, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(170) = new AttributeTypeInfo("voucher_serial_nr", 170, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(171) = new AttributeTypeInfo("origin_host_nm", 171, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(172) = new AttributeTypeInfo("external_data1", 172, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(173) = new AttributeTypeInfo("msg_unique_id_enrich", 173, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(174) = new AttributeTypeInfo("base_file_name", 174, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(175) = new AttributeTypeInfo("path", 175, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(176) = new AttributeTypeInfo("line_number", 176, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(177) = new AttributeTypeInfo("file_id", 177, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(178) = new AttributeTypeInfo("processed_timestamp", 178, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(179) = new AttributeTypeInfo("ltz_event_timestamp_enrich", 179, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(180) = new AttributeTypeInfo("kamanja_system_null_flags", 180, AttributeTypeInfo.TypeCategory.ARRAY, 7, -1, 0)


      return attributeTypes
    }
    

		 var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
				def setFn0(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.edrtype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field edrtype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn1(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.originnodetype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field originnodetype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn2(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.originhostname = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field originhostname in message CS5_AIR_REFILL_MA") 
				} 
				def setFn3(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.originfileid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field originfileid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn4(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.origintransactionid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field origintransactionid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn5(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.originoperatorid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field originoperatorid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn6(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.origintimestamp = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field origintimestamp in message CS5_AIR_REFILL_MA") 
				} 
				def setFn7(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.hostname = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field hostname in message CS5_AIR_REFILL_MA") 
				} 
				def setFn8(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.localsequencenumber = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field localsequencenumber in message CS5_AIR_REFILL_MA") 
				} 
				def setFn9(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.timestamp_v = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field timestamp_v in message CS5_AIR_REFILL_MA") 
				} 
				def setFn10(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.currentserviceclass = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field currentserviceclass in message CS5_AIR_REFILL_MA") 
				} 
				def setFn11(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.voucherbasedrefill = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field voucherbasedrefill in message CS5_AIR_REFILL_MA") 
				} 
				def setFn12(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.transactiontype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field transactiontype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn13(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.transactioncode = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field transactioncode in message CS5_AIR_REFILL_MA") 
				} 
				def setFn14(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.transactionamount = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field transactionamount in message CS5_AIR_REFILL_MA") 
				} 
				def setFn15(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.transactioncurrency = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field transactioncurrency in message CS5_AIR_REFILL_MA") 
				} 
				def setFn16(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.refillamountconverted = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field refillamountconverted in message CS5_AIR_REFILL_MA") 
				} 
				def setFn17(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.refilldivisionamount = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field refilldivisionamount in message CS5_AIR_REFILL_MA") 
				} 
				def setFn18(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.refilltype = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field refilltype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn19(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.refillprofileid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field refillprofileid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn20(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.segmentationid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field segmentationid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn21(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.voucherserialnumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field voucherserialnumber in message CS5_AIR_REFILL_MA") 
				} 
				def setFn22(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.vouchergroupid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field vouchergroupid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn23(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountnumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountnumber in message CS5_AIR_REFILL_MA") 
				} 
				def setFn24(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountcurrency = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountcurrency in message CS5_AIR_REFILL_MA") 
				} 
				def setFn25(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.subscribernumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field subscribernumber in message CS5_AIR_REFILL_MA") 
				} 
				def setFn26(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.promotionannouncementcode = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field promotionannouncementcode in message CS5_AIR_REFILL_MA") 
				} 
				def setFn27(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountflags = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountflags in message CS5_AIR_REFILL_MA") 
				} 
				def setFn28(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountbalance = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountbalance in message CS5_AIR_REFILL_MA") 
				} 
				def setFn29(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accumulatedrefillvalue = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accumulatedrefillvalue in message CS5_AIR_REFILL_MA") 
				} 
				def setFn30(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.accumulatedrefillcounter = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field accumulatedrefillcounter in message CS5_AIR_REFILL_MA") 
				} 
				def setFn31(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accumulatedprogressionvalue = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accumulatedprogressionvalue in message CS5_AIR_REFILL_MA") 
				} 
				def setFn32(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.accumulatedprogressioncounter = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field accumulatedprogressioncounter in message CS5_AIR_REFILL_MA") 
				} 
				def setFn33(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.creditclearanceperiod = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field creditclearanceperiod in message CS5_AIR_REFILL_MA") 
				} 
				def setFn34(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.promotionplan = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field promotionplan in message CS5_AIR_REFILL_MA") 
				} 
				def setFn35(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.permanentserviceclass = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field permanentserviceclass in message CS5_AIR_REFILL_MA") 
				} 
				def setFn36(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.temporaryserviceclass = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field temporaryserviceclass in message CS5_AIR_REFILL_MA") 
				} 
				def setFn37(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.temporaryserviceclassexpirydate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field temporaryserviceclassexpirydate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn38(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.refilloption = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field refilloption in message CS5_AIR_REFILL_MA") 
				} 
				def setFn39(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.servicefeeexpirydate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field servicefeeexpirydate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn40(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.serviceremovalgraceperiod = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field serviceremovalgraceperiod in message CS5_AIR_REFILL_MA") 
				} 
				def setFn41(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.serviceoffering = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field serviceoffering in message CS5_AIR_REFILL_MA") 
				} 
				def setFn42(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.supervisionexpirydate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field supervisionexpirydate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn43(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.communityid1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field communityid1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn44(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.communityid2 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field communityid2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn45(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.communityid3 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field communityid3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn46(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier in message CS5_AIR_REFILL_MA") 
				} 
				def setFn47(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn48(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn49(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn50(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn51(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn52(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn53(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn54(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____2 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn55(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn56(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn57(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn58(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____3 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn59(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn60(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn61(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn62(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____4 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn63(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn64(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn65(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn66(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountflags____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountflags____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn67(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountbalance____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountbalance____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn68(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accumulatedrefillvalue____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accumulatedrefillvalue____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn69(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.accumulatedrefillcounter____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field accumulatedrefillcounter____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn70(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accumulatedprogressionvalue____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accumulatedprogressionvalue____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn71(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.accumulatedprogressioncounter____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field accumulatedprogressioncounter____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn72(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.creditclearanceperiod____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field creditclearanceperiod____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn73(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.promotionplan____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field promotionplan____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn74(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.permanentserviceclass____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field permanentserviceclass____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn75(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.temporaryserviceclass____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field temporaryserviceclass____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn76(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.temporaryserviceclassexpirydate____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field temporaryserviceclassexpirydate____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn77(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.refilloption____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field refilloption____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn78(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.servicefeeexpirydate____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field servicefeeexpirydate____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn79(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.serviceremovalgraceperiod____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field serviceremovalgraceperiod____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn80(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.serviceoffering____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field serviceoffering____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn81(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.supervisionexpirydate____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field supervisionexpirydate____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn82(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.communityid1____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field communityid1____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn83(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.communityid2____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field communityid2____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn84(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.communityid3____1 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field communityid3____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn85(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____5 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____5 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn86(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____5 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____5 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn87(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____5 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____5 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn88(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____5 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____5 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn89(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____6 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____6 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn90(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____6 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____6 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn91(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____6 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____6 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn92(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____6 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____6 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn93(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____7 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____7 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn94(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____7 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____7 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn95(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____7 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____7 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn96(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____7 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____7 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn97(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____8 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____8 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn98(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____8 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____8 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn99(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____8 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____8 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn100(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____8 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____8 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn101(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.offeridentifier____9 = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field offeridentifier____9 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn102(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerstartdate____9 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerstartdate____9 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn103(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offerexpirydate____9 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offerexpirydate____9 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn104(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.offertype____9 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field offertype____9 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn105(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.refillpromodivisionamount = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field refillpromodivisionamount in message CS5_AIR_REFILL_MA") 
				} 
				def setFn106(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.supervisiondayspromopart = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field supervisiondayspromopart in message CS5_AIR_REFILL_MA") 
				} 
				def setFn107(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.supervisiondayssurplus = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field supervisiondayssurplus in message CS5_AIR_REFILL_MA") 
				} 
				def setFn108(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.servicefeedayspromopart = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field servicefeedayspromopart in message CS5_AIR_REFILL_MA") 
				} 
				def setFn109(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.servicefeedayssurplus = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field servicefeedayssurplus in message CS5_AIR_REFILL_MA") 
				} 
				def setFn110(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.maximumservicefeeperiod = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field maximumservicefeeperiod in message CS5_AIR_REFILL_MA") 
				} 
				def setFn111(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.maximumsupervisionperiod = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field maximumsupervisionperiod in message CS5_AIR_REFILL_MA") 
				} 
				def setFn112(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.activationdate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field activationdate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn113(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.welcomestatus = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field welcomestatus in message CS5_AIR_REFILL_MA") 
				} 
				def setFn114(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.voucheragent = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field voucheragent in message CS5_AIR_REFILL_MA") 
				} 
				def setFn115(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.promotionplanallocstartdate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field promotionplanallocstartdate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn116(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.accountgroupid = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field accountgroupid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn117(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.externaldata1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field externaldata1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn118(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.externaldata2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field externaldata2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn119(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.externaldata3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field externaldata3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn120(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.externaldata4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field externaldata4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn121(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.locationnumber = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field locationnumber in message CS5_AIR_REFILL_MA") 
				} 
				def setFn122(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.voucheractivationcode = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field voucheractivationcode in message CS5_AIR_REFILL_MA") 
				} 
				def setFn123(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accountcurrencycleared = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accountcurrencycleared in message CS5_AIR_REFILL_MA") 
				} 
				def setFn124(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.ignoreserviceclasshierarchy = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field ignoreserviceclasshierarchy in message CS5_AIR_REFILL_MA") 
				} 
				def setFn125(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn126(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeversion = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeversion in message CS5_AIR_REFILL_MA") 
				} 
				def setFn127(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeid____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeid____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn128(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeversion____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeversion____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn129(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeid____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeid____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn130(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeversion____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeversion____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn131(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeid____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeid____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn132(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeversion____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeversion____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn133(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeid____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeid____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn134(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.selectiontreeversion____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field selectiontreeversion____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn135(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parameterid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parameterid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn136(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parametervalue = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parametervalue in message CS5_AIR_REFILL_MA") 
				} 
				def setFn137(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parameterid____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parameterid____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn138(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parametervalue____1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parametervalue____1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn139(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parameterid____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parameterid____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn140(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parametervalue____2 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parametervalue____2 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn141(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parameterid____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parameterid____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn142(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parametervalue____3 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parametervalue____3 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn143(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parameterid____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parameterid____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn144(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.parametervalue____4 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field parametervalue____4 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn145(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.accounthomeregion = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field accounthomeregion in message CS5_AIR_REFILL_MA") 
				} 
				def setFn146(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.subscriberregion = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field subscriberregion in message CS5_AIR_REFILL_MA") 
				} 
				def setFn147(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.voucherregion = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field voucherregion in message CS5_AIR_REFILL_MA") 
				} 
				def setFn148(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.promotionplanallocenddate = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field promotionplanallocenddate in message CS5_AIR_REFILL_MA") 
				} 
				def setFn149(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.requestedrefilltype = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field requestedrefilltype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn150(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.cellglobalid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field cellglobalid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn151(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.dasizeafter = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field dasizeafter in message CS5_AIR_REFILL_MA") 
				} 
				def setFn152(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.dasizebefore = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field dasizebefore in message CS5_AIR_REFILL_MA") 
				} 
				def setFn153(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accsizebefore = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accsizebefore in message CS5_AIR_REFILL_MA") 
				} 
				def setFn154(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.accsizeafter = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field accsizeafter in message CS5_AIR_REFILL_MA") 
				} 
				def setFn155(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.serviceclassid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field serviceclassid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn156(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.paytype = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field paytype in message CS5_AIR_REFILL_MA") 
				} 
				def setFn157(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.callreferenceid = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field callreferenceid in message CS5_AIR_REFILL_MA") 
				} 
				def setFn158(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.mainamountused = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field mainamountused in message CS5_AIR_REFILL_MA") 
				} 
				def setFn159(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.file_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field file_name in message CS5_AIR_REFILL_MA") 
				} 
				def setFn160(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.file_offset = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field file_offset in message CS5_AIR_REFILL_MA") 
				} 
				def setFn161(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.kamanja_loaded_date = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field kamanja_loaded_date in message CS5_AIR_REFILL_MA") 
				} 
				def setFn162(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.file_mod_date = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field file_mod_date in message CS5_AIR_REFILL_MA") 
				} 
				def setFn163(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.recharge_type_enrich = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field recharge_type_enrich in message CS5_AIR_REFILL_MA") 
				} 
				def setFn164(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]){ 
				  curObj.date_key = value.asInstanceOf[Int]; 
				  curObj.setTimePartitionData; 
				} else throw new Exception(s"Value is the not the correct type Int for field date_key in message CS5_AIR_REFILL_MA") 
				} 
				def setFn165(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.msisdn_key = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field msisdn_key in message CS5_AIR_REFILL_MA") 
				} 
				def setFn166(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.event_timestamp_enrich = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field event_timestamp_enrich in message CS5_AIR_REFILL_MA") 
				} 
				def setFn167(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.original_timestamp_enrich = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field original_timestamp_enrich in message CS5_AIR_REFILL_MA") 
				} 
				def setFn168(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Double]) 
				  curObj.transaction_amt = value.asInstanceOf[Double]; 
				 else throw new Exception(s"Value is the not the correct type Double for field transaction_amt in message CS5_AIR_REFILL_MA") 
				} 
				def setFn169(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.origin_time_stamp = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field origin_time_stamp in message CS5_AIR_REFILL_MA") 
				} 
				def setFn170(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.voucher_serial_nr = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field voucher_serial_nr in message CS5_AIR_REFILL_MA") 
				} 
				def setFn171(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.origin_host_nm = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field origin_host_nm in message CS5_AIR_REFILL_MA") 
				} 
				def setFn172(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.external_data1 = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field external_data1 in message CS5_AIR_REFILL_MA") 
				} 
				def setFn173(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.msg_unique_id_enrich = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field msg_unique_id_enrich in message CS5_AIR_REFILL_MA") 
				} 
				def setFn174(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.base_file_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field base_file_name in message CS5_AIR_REFILL_MA") 
				} 
				def setFn175(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.path = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field path in message CS5_AIR_REFILL_MA") 
				} 
				def setFn176(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.line_number = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field line_number in message CS5_AIR_REFILL_MA") 
				} 
				def setFn177(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.file_id = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field file_id in message CS5_AIR_REFILL_MA") 
				} 
				def setFn178(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.processed_timestamp = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field processed_timestamp in message CS5_AIR_REFILL_MA") 
				} 
				def setFn179(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.ltz_event_timestamp_enrich = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field ltz_event_timestamp_enrich in message CS5_AIR_REFILL_MA") 
				} 
				def setFn180(curObj: CS5_AIR_REFILL_MA, value: Any): Unit ={ 
				if(value.isInstanceOf[scala.Array[Boolean]]) 
				  curObj.kamanja_system_null_flags = value.asInstanceOf[scala.Array[Boolean]]; 
				else if(value.isInstanceOf[scala.Array[_]]) 
				  curObj.kamanja_system_null_flags = value.asInstanceOf[scala.Array[_]].map(v => v.asInstanceOf[Boolean]); 
				 else throw new Exception(s"Value is the not the correct type scala.Array[Boolean]/Array[_] for field kamanja_system_null_flags in message CS5_AIR_REFILL_MA") 
				} 

    val setFnArr = Array[(CS5_AIR_REFILL_MA, Any) => Unit](setFn0,setFn1,setFn2,setFn3,setFn4,setFn5,setFn6,setFn7,setFn8,setFn9,setFn10,setFn11,setFn12,setFn13,setFn14,setFn15,setFn16,setFn17,setFn18,setFn19,setFn20,setFn21,setFn22,setFn23,setFn24,setFn25,setFn26,setFn27,setFn28,setFn29,setFn30,setFn31,setFn32,setFn33,setFn34,setFn35,setFn36,setFn37,setFn38,setFn39,setFn40,setFn41,setFn42,setFn43,setFn44,setFn45,setFn46,setFn47,setFn48,setFn49,setFn50,setFn51,setFn52,setFn53,setFn54,setFn55,setFn56,setFn57,setFn58,setFn59,setFn60,setFn61,setFn62,setFn63,setFn64,setFn65,setFn66,setFn67,setFn68,setFn69,setFn70,setFn71,setFn72,setFn73,setFn74,setFn75,setFn76,setFn77,setFn78,setFn79,setFn80,setFn81,setFn82,setFn83,setFn84,setFn85,setFn86,setFn87,setFn88,setFn89,setFn90,setFn91,setFn92,setFn93,setFn94,setFn95,setFn96,setFn97,setFn98,setFn99,setFn100,setFn101,setFn102,setFn103,setFn104,setFn105,setFn106,setFn107,setFn108,setFn109,setFn110,setFn111,setFn112,setFn113,setFn114,setFn115,setFn116,setFn117,setFn118,setFn119,setFn120,setFn121,setFn122,setFn123,setFn124,setFn125,setFn126,setFn127,setFn128,setFn129,setFn130,setFn131,setFn132,setFn133,setFn134,setFn135,setFn136,setFn137,setFn138,setFn139,setFn140,setFn141,setFn142,setFn143,setFn144,setFn145,setFn146,setFn147,setFn148,setFn149,setFn150,setFn151,setFn152,setFn153,setFn154,setFn155,setFn156,setFn157,setFn158,setFn159,setFn160,setFn161,setFn162,setFn163,setFn164,setFn165,setFn166,setFn167,setFn168,setFn169,setFn170,setFn171,setFn172,setFn173,setFn174,setFn175,setFn176,setFn177,setFn178,setFn179,setFn180    )
		def getFn0(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.edrtype.asInstanceOf[AnyRef]; 
		def getFn1(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.originnodetype.asInstanceOf[AnyRef]; 
		def getFn2(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.originhostname.asInstanceOf[AnyRef]; 
		def getFn3(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.originfileid.asInstanceOf[AnyRef]; 
		def getFn4(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.origintransactionid.asInstanceOf[AnyRef]; 
		def getFn5(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.originoperatorid.asInstanceOf[AnyRef]; 
		def getFn6(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.origintimestamp.asInstanceOf[AnyRef]; 
		def getFn7(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.hostname.asInstanceOf[AnyRef]; 
		def getFn8(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.localsequencenumber.asInstanceOf[AnyRef]; 
		def getFn9(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.timestamp_v.asInstanceOf[AnyRef]; 
		def getFn10(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.currentserviceclass.asInstanceOf[AnyRef]; 
		def getFn11(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.voucherbasedrefill.asInstanceOf[AnyRef]; 
		def getFn12(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.transactiontype.asInstanceOf[AnyRef]; 
		def getFn13(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.transactioncode.asInstanceOf[AnyRef]; 
		def getFn14(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.transactionamount.asInstanceOf[AnyRef]; 
		def getFn15(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.transactioncurrency.asInstanceOf[AnyRef]; 
		def getFn16(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refillamountconverted.asInstanceOf[AnyRef]; 
		def getFn17(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refilldivisionamount.asInstanceOf[AnyRef]; 
		def getFn18(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refilltype.asInstanceOf[AnyRef]; 
		def getFn19(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refillprofileid.asInstanceOf[AnyRef]; 
		def getFn20(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.segmentationid.asInstanceOf[AnyRef]; 
		def getFn21(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.voucherserialnumber.asInstanceOf[AnyRef]; 
		def getFn22(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.vouchergroupid.asInstanceOf[AnyRef]; 
		def getFn23(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountnumber.asInstanceOf[AnyRef]; 
		def getFn24(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountcurrency.asInstanceOf[AnyRef]; 
		def getFn25(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.subscribernumber.asInstanceOf[AnyRef]; 
		def getFn26(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.promotionannouncementcode.asInstanceOf[AnyRef]; 
		def getFn27(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountflags.asInstanceOf[AnyRef]; 
		def getFn28(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountbalance.asInstanceOf[AnyRef]; 
		def getFn29(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedrefillvalue.asInstanceOf[AnyRef]; 
		def getFn30(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedrefillcounter.asInstanceOf[AnyRef]; 
		def getFn31(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedprogressionvalue.asInstanceOf[AnyRef]; 
		def getFn32(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedprogressioncounter.asInstanceOf[AnyRef]; 
		def getFn33(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.creditclearanceperiod.asInstanceOf[AnyRef]; 
		def getFn34(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.promotionplan.asInstanceOf[AnyRef]; 
		def getFn35(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.permanentserviceclass.asInstanceOf[AnyRef]; 
		def getFn36(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.temporaryserviceclass.asInstanceOf[AnyRef]; 
		def getFn37(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.temporaryserviceclassexpirydate.asInstanceOf[AnyRef]; 
		def getFn38(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refilloption.asInstanceOf[AnyRef]; 
		def getFn39(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.servicefeeexpirydate.asInstanceOf[AnyRef]; 
		def getFn40(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.serviceremovalgraceperiod.asInstanceOf[AnyRef]; 
		def getFn41(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.serviceoffering.asInstanceOf[AnyRef]; 
		def getFn42(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.supervisionexpirydate.asInstanceOf[AnyRef]; 
		def getFn43(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.communityid1.asInstanceOf[AnyRef]; 
		def getFn44(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.communityid2.asInstanceOf[AnyRef]; 
		def getFn45(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.communityid3.asInstanceOf[AnyRef]; 
		def getFn46(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier.asInstanceOf[AnyRef]; 
		def getFn47(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate.asInstanceOf[AnyRef]; 
		def getFn48(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate.asInstanceOf[AnyRef]; 
		def getFn49(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype.asInstanceOf[AnyRef]; 
		def getFn50(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____1.asInstanceOf[AnyRef]; 
		def getFn51(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____1.asInstanceOf[AnyRef]; 
		def getFn52(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____1.asInstanceOf[AnyRef]; 
		def getFn53(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____1.asInstanceOf[AnyRef]; 
		def getFn54(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____2.asInstanceOf[AnyRef]; 
		def getFn55(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____2.asInstanceOf[AnyRef]; 
		def getFn56(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____2.asInstanceOf[AnyRef]; 
		def getFn57(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____2.asInstanceOf[AnyRef]; 
		def getFn58(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____3.asInstanceOf[AnyRef]; 
		def getFn59(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____3.asInstanceOf[AnyRef]; 
		def getFn60(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____3.asInstanceOf[AnyRef]; 
		def getFn61(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____3.asInstanceOf[AnyRef]; 
		def getFn62(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____4.asInstanceOf[AnyRef]; 
		def getFn63(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____4.asInstanceOf[AnyRef]; 
		def getFn64(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____4.asInstanceOf[AnyRef]; 
		def getFn65(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____4.asInstanceOf[AnyRef]; 
		def getFn66(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountflags____1.asInstanceOf[AnyRef]; 
		def getFn67(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountbalance____1.asInstanceOf[AnyRef]; 
		def getFn68(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedrefillvalue____1.asInstanceOf[AnyRef]; 
		def getFn69(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedrefillcounter____1.asInstanceOf[AnyRef]; 
		def getFn70(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedprogressionvalue____1.asInstanceOf[AnyRef]; 
		def getFn71(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accumulatedprogressioncounter____1.asInstanceOf[AnyRef]; 
		def getFn72(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.creditclearanceperiod____1.asInstanceOf[AnyRef]; 
		def getFn73(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.promotionplan____1.asInstanceOf[AnyRef]; 
		def getFn74(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.permanentserviceclass____1.asInstanceOf[AnyRef]; 
		def getFn75(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.temporaryserviceclass____1.asInstanceOf[AnyRef]; 
		def getFn76(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.temporaryserviceclassexpirydate____1.asInstanceOf[AnyRef]; 
		def getFn77(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refilloption____1.asInstanceOf[AnyRef]; 
		def getFn78(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.servicefeeexpirydate____1.asInstanceOf[AnyRef]; 
		def getFn79(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.serviceremovalgraceperiod____1.asInstanceOf[AnyRef]; 
		def getFn80(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.serviceoffering____1.asInstanceOf[AnyRef]; 
		def getFn81(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.supervisionexpirydate____1.asInstanceOf[AnyRef]; 
		def getFn82(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.communityid1____1.asInstanceOf[AnyRef]; 
		def getFn83(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.communityid2____1.asInstanceOf[AnyRef]; 
		def getFn84(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.communityid3____1.asInstanceOf[AnyRef]; 
		def getFn85(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____5.asInstanceOf[AnyRef]; 
		def getFn86(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____5.asInstanceOf[AnyRef]; 
		def getFn87(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____5.asInstanceOf[AnyRef]; 
		def getFn88(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____5.asInstanceOf[AnyRef]; 
		def getFn89(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____6.asInstanceOf[AnyRef]; 
		def getFn90(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____6.asInstanceOf[AnyRef]; 
		def getFn91(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____6.asInstanceOf[AnyRef]; 
		def getFn92(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____6.asInstanceOf[AnyRef]; 
		def getFn93(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____7.asInstanceOf[AnyRef]; 
		def getFn94(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____7.asInstanceOf[AnyRef]; 
		def getFn95(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____7.asInstanceOf[AnyRef]; 
		def getFn96(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____7.asInstanceOf[AnyRef]; 
		def getFn97(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____8.asInstanceOf[AnyRef]; 
		def getFn98(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____8.asInstanceOf[AnyRef]; 
		def getFn99(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____8.asInstanceOf[AnyRef]; 
		def getFn100(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____8.asInstanceOf[AnyRef]; 
		def getFn101(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offeridentifier____9.asInstanceOf[AnyRef]; 
		def getFn102(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerstartdate____9.asInstanceOf[AnyRef]; 
		def getFn103(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offerexpirydate____9.asInstanceOf[AnyRef]; 
		def getFn104(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.offertype____9.asInstanceOf[AnyRef]; 
		def getFn105(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.refillpromodivisionamount.asInstanceOf[AnyRef]; 
		def getFn106(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.supervisiondayspromopart.asInstanceOf[AnyRef]; 
		def getFn107(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.supervisiondayssurplus.asInstanceOf[AnyRef]; 
		def getFn108(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.servicefeedayspromopart.asInstanceOf[AnyRef]; 
		def getFn109(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.servicefeedayssurplus.asInstanceOf[AnyRef]; 
		def getFn110(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.maximumservicefeeperiod.asInstanceOf[AnyRef]; 
		def getFn111(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.maximumsupervisionperiod.asInstanceOf[AnyRef]; 
		def getFn112(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.activationdate.asInstanceOf[AnyRef]; 
		def getFn113(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.welcomestatus.asInstanceOf[AnyRef]; 
		def getFn114(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.voucheragent.asInstanceOf[AnyRef]; 
		def getFn115(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.promotionplanallocstartdate.asInstanceOf[AnyRef]; 
		def getFn116(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountgroupid.asInstanceOf[AnyRef]; 
		def getFn117(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.externaldata1.asInstanceOf[AnyRef]; 
		def getFn118(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.externaldata2.asInstanceOf[AnyRef]; 
		def getFn119(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.externaldata3.asInstanceOf[AnyRef]; 
		def getFn120(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.externaldata4.asInstanceOf[AnyRef]; 
		def getFn121(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.locationnumber.asInstanceOf[AnyRef]; 
		def getFn122(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.voucheractivationcode.asInstanceOf[AnyRef]; 
		def getFn123(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accountcurrencycleared.asInstanceOf[AnyRef]; 
		def getFn124(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.ignoreserviceclasshierarchy.asInstanceOf[AnyRef]; 
		def getFn125(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeid.asInstanceOf[AnyRef]; 
		def getFn126(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeversion.asInstanceOf[AnyRef]; 
		def getFn127(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeid____1.asInstanceOf[AnyRef]; 
		def getFn128(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeversion____1.asInstanceOf[AnyRef]; 
		def getFn129(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeid____2.asInstanceOf[AnyRef]; 
		def getFn130(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeversion____2.asInstanceOf[AnyRef]; 
		def getFn131(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeid____3.asInstanceOf[AnyRef]; 
		def getFn132(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeversion____3.asInstanceOf[AnyRef]; 
		def getFn133(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeid____4.asInstanceOf[AnyRef]; 
		def getFn134(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.selectiontreeversion____4.asInstanceOf[AnyRef]; 
		def getFn135(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parameterid.asInstanceOf[AnyRef]; 
		def getFn136(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parametervalue.asInstanceOf[AnyRef]; 
		def getFn137(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parameterid____1.asInstanceOf[AnyRef]; 
		def getFn138(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parametervalue____1.asInstanceOf[AnyRef]; 
		def getFn139(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parameterid____2.asInstanceOf[AnyRef]; 
		def getFn140(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parametervalue____2.asInstanceOf[AnyRef]; 
		def getFn141(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parameterid____3.asInstanceOf[AnyRef]; 
		def getFn142(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parametervalue____3.asInstanceOf[AnyRef]; 
		def getFn143(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parameterid____4.asInstanceOf[AnyRef]; 
		def getFn144(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.parametervalue____4.asInstanceOf[AnyRef]; 
		def getFn145(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accounthomeregion.asInstanceOf[AnyRef]; 
		def getFn146(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.subscriberregion.asInstanceOf[AnyRef]; 
		def getFn147(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.voucherregion.asInstanceOf[AnyRef]; 
		def getFn148(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.promotionplanallocenddate.asInstanceOf[AnyRef]; 
		def getFn149(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.requestedrefilltype.asInstanceOf[AnyRef]; 
		def getFn150(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.cellglobalid.asInstanceOf[AnyRef]; 
		def getFn151(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.dasizeafter.asInstanceOf[AnyRef]; 
		def getFn152(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.dasizebefore.asInstanceOf[AnyRef]; 
		def getFn153(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accsizebefore.asInstanceOf[AnyRef]; 
		def getFn154(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.accsizeafter.asInstanceOf[AnyRef]; 
		def getFn155(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.serviceclassid.asInstanceOf[AnyRef]; 
		def getFn156(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.paytype.asInstanceOf[AnyRef]; 
		def getFn157(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.callreferenceid.asInstanceOf[AnyRef]; 
		def getFn158(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.mainamountused.asInstanceOf[AnyRef]; 
		def getFn159(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.file_name.asInstanceOf[AnyRef]; 
		def getFn160(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.file_offset.asInstanceOf[AnyRef]; 
		def getFn161(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.kamanja_loaded_date.asInstanceOf[AnyRef]; 
		def getFn162(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.file_mod_date.asInstanceOf[AnyRef]; 
		def getFn163(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.recharge_type_enrich.asInstanceOf[AnyRef]; 
		def getFn164(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.date_key.asInstanceOf[AnyRef]; 
		def getFn165(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.msisdn_key.asInstanceOf[AnyRef]; 
		def getFn166(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.event_timestamp_enrich.asInstanceOf[AnyRef]; 
		def getFn167(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.original_timestamp_enrich.asInstanceOf[AnyRef]; 
		def getFn168(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.transaction_amt.asInstanceOf[AnyRef]; 
		def getFn169(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.origin_time_stamp.asInstanceOf[AnyRef]; 
		def getFn170(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.voucher_serial_nr.asInstanceOf[AnyRef]; 
		def getFn171(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.origin_host_nm.asInstanceOf[AnyRef]; 
		def getFn172(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.external_data1.asInstanceOf[AnyRef]; 
		def getFn173(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.msg_unique_id_enrich.asInstanceOf[AnyRef]; 
		def getFn174(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.base_file_name.asInstanceOf[AnyRef]; 
		def getFn175(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.path.asInstanceOf[AnyRef]; 
		def getFn176(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.line_number.asInstanceOf[AnyRef]; 
		def getFn177(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.file_id.asInstanceOf[AnyRef]; 
		def getFn178(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.processed_timestamp.asInstanceOf[AnyRef]; 
		def getFn179(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.ltz_event_timestamp_enrich.asInstanceOf[AnyRef]; 
		def getFn180(curObj: CS5_AIR_REFILL_MA): AnyRef = curObj.kamanja_system_null_flags.asInstanceOf[AnyRef]; 

    val getFnArr = Array[(CS5_AIR_REFILL_MA) => AnyRef](getFn0,getFn1,getFn2,getFn3,getFn4,getFn5,getFn6,getFn7,getFn8,getFn9,getFn10,getFn11,getFn12,getFn13,getFn14,getFn15,getFn16,getFn17,getFn18,getFn19,getFn20,getFn21,getFn22,getFn23,getFn24,getFn25,getFn26,getFn27,getFn28,getFn29,getFn30,getFn31,getFn32,getFn33,getFn34,getFn35,getFn36,getFn37,getFn38,getFn39,getFn40,getFn41,getFn42,getFn43,getFn44,getFn45,getFn46,getFn47,getFn48,getFn49,getFn50,getFn51,getFn52,getFn53,getFn54,getFn55,getFn56,getFn57,getFn58,getFn59,getFn60,getFn61,getFn62,getFn63,getFn64,getFn65,getFn66,getFn67,getFn68,getFn69,getFn70,getFn71,getFn72,getFn73,getFn74,getFn75,getFn76,getFn77,getFn78,getFn79,getFn80,getFn81,getFn82,getFn83,getFn84,getFn85,getFn86,getFn87,getFn88,getFn89,getFn90,getFn91,getFn92,getFn93,getFn94,getFn95,getFn96,getFn97,getFn98,getFn99,getFn100,getFn101,getFn102,getFn103,getFn104,getFn105,getFn106,getFn107,getFn108,getFn109,getFn110,getFn111,getFn112,getFn113,getFn114,getFn115,getFn116,getFn117,getFn118,getFn119,getFn120,getFn121,getFn122,getFn123,getFn124,getFn125,getFn126,getFn127,getFn128,getFn129,getFn130,getFn131,getFn132,getFn133,getFn134,getFn135,getFn136,getFn137,getFn138,getFn139,getFn140,getFn141,getFn142,getFn143,getFn144,getFn145,getFn146,getFn147,getFn148,getFn149,getFn150,getFn151,getFn152,getFn153,getFn154,getFn155,getFn156,getFn157,getFn158,getFn159,getFn160,getFn161,getFn162,getFn163,getFn164,getFn165,getFn166,getFn167,getFn168,getFn169,getFn170,getFn171,getFn172,getFn173,getFn174,getFn175,getFn176,getFn177,getFn178,getFn179,getFn180    )



    override def getRDDObject(): RDDObject[ContainerInterface] = CS5_AIR_REFILL_MA.asInstanceOf[RDDObject[ContainerInterface]];


    
    final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);
      
    override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
      try {
        if (oldVerobj == null) return null;
        oldVerobj match {
          
      case oldVerobj: com.mtn.messages.CS5_AIR_REFILL_MA => { return  convertToVer1000000(oldVerobj); } 
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
  
    private def convertToVer1000000(oldVerobj: com.mtn.messages.CS5_AIR_REFILL_MA): com.mtn.messages.CS5_AIR_REFILL_MA= {
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
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj CS5_AIR_REFILL_MA") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj CS5_AIR_REFILL_MA");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj CS5_AIR_REFILL_MA");
 override def NeedToTransformData: Boolean = false
    }

class CS5_AIR_REFILL_MA(factory: MessageFactoryInterface, other: CS5_AIR_REFILL_MA) extends MessageInterface(factory) { 
 
  val log = CS5_AIR_REFILL_MA.log

      var attributeTypes = CS5_AIR_REFILL_MA.attributeTypes
      
		 var keyTypes = CS5_AIR_REFILL_MA.keyTypes;
    
     if (other != null && other != this) {
      // call copying fields from other to local variables
      fromFunc(other)
    }
    
    override def save: Unit = { CS5_AIR_REFILL_MA.saveOne(this) }
  
    def Clone(): ContainerOrConcept = { CS5_AIR_REFILL_MA.build(this) }

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
  
  
      var setFnArr = CS5_AIR_REFILL_MA.setFnArr
    
      var getFnArr = CS5_AIR_REFILL_MA.getFnArr
    
 		var edrtype : String = _; 
 		var originnodetype : String = _; 
 		var originhostname : String = _; 
 		var originfileid : String = _; 
 		var origintransactionid : String = _; 
 		var originoperatorid : String = _; 
 		var origintimestamp : String = _; 
 		var hostname : String = _; 
 		var localsequencenumber : Int = _; 
 		var timestamp_v : Long = _; 
 		var currentserviceclass : Int = _; 
 		var voucherbasedrefill : String = _; 
 		var transactiontype : String = _; 
 		var transactioncode : String = _; 
 		var transactionamount : String = _; 
 		var transactioncurrency : String = _; 
 		var refillamountconverted : String = _; 
 		var refilldivisionamount : String = _; 
 		var refilltype : Int = _; 
 		var refillprofileid : String = _; 
 		var segmentationid : String = _; 
 		var voucherserialnumber : String = _; 
 		var vouchergroupid : String = _; 
 		var accountnumber : String = _; 
 		var accountcurrency : String = _; 
 		var subscribernumber : String = _; 
 		var promotionannouncementcode : Int = _; 
 		var accountflags : String = _; 
 		var accountbalance : String = _; 
 		var accumulatedrefillvalue : String = _; 
 		var accumulatedrefillcounter : Int = _; 
 		var accumulatedprogressionvalue : String = _; 
 		var accumulatedprogressioncounter : Int = _; 
 		var creditclearanceperiod : Int = _; 
 		var promotionplan : String = _; 
 		var permanentserviceclass : Int = _; 
 		var temporaryserviceclass : Int = _; 
 		var temporaryserviceclassexpirydate : String = _; 
 		var refilloption : Int = _; 
 		var servicefeeexpirydate : String = _; 
 		var serviceremovalgraceperiod : Int = _; 
 		var serviceoffering : Int = _; 
 		var supervisionexpirydate : String = _; 
 		var communityid1 : Int = _; 
 		var communityid2 : Int = _; 
 		var communityid3 : Int = _; 
 		var offeridentifier : Int = _; 
 		var offerstartdate : String = _; 
 		var offerexpirydate : String = _; 
 		var offertype : String = _; 
 		var offeridentifier____1 : Int = _; 
 		var offerstartdate____1 : String = _; 
 		var offerexpirydate____1 : String = _; 
 		var offertype____1 : String = _; 
 		var offeridentifier____2 : Int = _; 
 		var offerstartdate____2 : String = _; 
 		var offerexpirydate____2 : String = _; 
 		var offertype____2 : String = _; 
 		var offeridentifier____3 : Int = _; 
 		var offerstartdate____3 : String = _; 
 		var offerexpirydate____3 : String = _; 
 		var offertype____3 : String = _; 
 		var offeridentifier____4 : Int = _; 
 		var offerstartdate____4 : String = _; 
 		var offerexpirydate____4 : String = _; 
 		var offertype____4 : String = _; 
 		var accountflags____1 : String = _; 
 		var accountbalance____1 : String = _; 
 		var accumulatedrefillvalue____1 : String = _; 
 		var accumulatedrefillcounter____1 : Int = _; 
 		var accumulatedprogressionvalue____1 : String = _; 
 		var accumulatedprogressioncounter____1 : Int = _; 
 		var creditclearanceperiod____1 : Int = _; 
 		var promotionplan____1 : String = _; 
 		var permanentserviceclass____1 : Int = _; 
 		var temporaryserviceclass____1 : Int = _; 
 		var temporaryserviceclassexpirydate____1 : String = _; 
 		var refilloption____1 : Int = _; 
 		var servicefeeexpirydate____1 : String = _; 
 		var serviceremovalgraceperiod____1 : Int = _; 
 		var serviceoffering____1 : Int = _; 
 		var supervisionexpirydate____1 : String = _; 
 		var communityid1____1 : Int = _; 
 		var communityid2____1 : Int = _; 
 		var communityid3____1 : Int = _; 
 		var offeridentifier____5 : Int = _; 
 		var offerstartdate____5 : String = _; 
 		var offerexpirydate____5 : String = _; 
 		var offertype____5 : String = _; 
 		var offeridentifier____6 : Int = _; 
 		var offerstartdate____6 : String = _; 
 		var offerexpirydate____6 : String = _; 
 		var offertype____6 : String = _; 
 		var offeridentifier____7 : Int = _; 
 		var offerstartdate____7 : String = _; 
 		var offerexpirydate____7 : String = _; 
 		var offertype____7 : String = _; 
 		var offeridentifier____8 : Int = _; 
 		var offerstartdate____8 : String = _; 
 		var offerexpirydate____8 : String = _; 
 		var offertype____8 : String = _; 
 		var offeridentifier____9 : Int = _; 
 		var offerstartdate____9 : String = _; 
 		var offerexpirydate____9 : String = _; 
 		var offertype____9 : String = _; 
 		var refillpromodivisionamount : String = _; 
 		var supervisiondayspromopart : Int = _; 
 		var supervisiondayssurplus : Int = _; 
 		var servicefeedayspromopart : Int = _; 
 		var servicefeedayssurplus : Int = _; 
 		var maximumservicefeeperiod : Int = _; 
 		var maximumsupervisionperiod : Int = _; 
 		var activationdate : String = _; 
 		var welcomestatus : String = _; 
 		var voucheragent : String = _; 
 		var promotionplanallocstartdate : String = _; 
 		var accountgroupid : Int = _; 
 		var externaldata1 : String = _; 
 		var externaldata2 : String = _; 
 		var externaldata3 : String = _; 
 		var externaldata4 : String = _; 
 		var locationnumber : String = _; 
 		var voucheractivationcode : String = _; 
 		var accountcurrencycleared : String = _; 
 		var ignoreserviceclasshierarchy : String = _; 
 		var selectiontreeid : String = _; 
 		var selectiontreeversion : String = _; 
 		var selectiontreeid____1 : String = _; 
 		var selectiontreeversion____1 : String = _; 
 		var selectiontreeid____2 : String = _; 
 		var selectiontreeversion____2 : String = _; 
 		var selectiontreeid____3 : String = _; 
 		var selectiontreeversion____3 : String = _; 
 		var selectiontreeid____4 : String = _; 
 		var selectiontreeversion____4 : String = _; 
 		var parameterid : String = _; 
 		var parametervalue : String = _; 
 		var parameterid____1 : String = _; 
 		var parametervalue____1 : String = _; 
 		var parameterid____2 : String = _; 
 		var parametervalue____2 : String = _; 
 		var parameterid____3 : String = _; 
 		var parametervalue____3 : String = _; 
 		var parameterid____4 : String = _; 
 		var parametervalue____4 : String = _; 
 		var accounthomeregion : Int = _; 
 		var subscriberregion : Int = _; 
 		var voucherregion : Int = _; 
 		var promotionplanallocenddate : String = _; 
 		var requestedrefilltype : Int = _; 
 		var cellglobalid : String = _; 
 		var dasizeafter : String = _; 
 		var dasizebefore : String = _; 
 		var accsizebefore : String = _; 
 		var accsizeafter : String = _; 
 		var serviceclassid : String = _; 
 		var paytype : String = _; 
 		var callreferenceid : String = _; 
 		var mainamountused : String = _; 
 		var file_name : String = _; 
 		var file_offset : Long = _; 
 		var kamanja_loaded_date : String = _; 
 		var file_mod_date : String = _; 
 		var recharge_type_enrich : Int = _; 
 		var date_key : Int = _; 
 		var msisdn_key : Long = _; 
 		var event_timestamp_enrich : Long = _; 
 		var original_timestamp_enrich : String = _; 
 		var transaction_amt : Double = _; 
 		var origin_time_stamp : String = _; 
 		var voucher_serial_nr : String = _; 
 		var origin_host_nm : String = _; 
 		var external_data1 : String = _; 
 		var msg_unique_id_enrich : Long = _; 
 		var base_file_name : String = _; 
 		var path : String = _; 
 		var line_number : Long = _; 
 		var file_id : String = _; 
 		var processed_timestamp : Long = _; 
 		var ltz_event_timestamp_enrich : Long = _; 
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
      val fieldX = ru.typeOf[CS5_AIR_REFILL_MA].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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
   
      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container CS5_AIR_REFILL_MA", null);
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
      throw new Exception(s"$index is out of range for message CS5_AIR_REFILL_MA");
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
      var attributeVals = new Array[AttributeValue](181);
      try{
 				attributeVals(0) = new AttributeValue(this.edrtype, attributeTypes(0)) 
				attributeVals(1) = new AttributeValue(this.originnodetype, attributeTypes(1)) 
				attributeVals(2) = new AttributeValue(this.originhostname, attributeTypes(2)) 
				attributeVals(3) = new AttributeValue(this.originfileid, attributeTypes(3)) 
				attributeVals(4) = new AttributeValue(this.origintransactionid, attributeTypes(4)) 
				attributeVals(5) = new AttributeValue(this.originoperatorid, attributeTypes(5)) 
				attributeVals(6) = new AttributeValue(this.origintimestamp, attributeTypes(6)) 
				attributeVals(7) = new AttributeValue(this.hostname, attributeTypes(7)) 
				attributeVals(8) = new AttributeValue(this.localsequencenumber, attributeTypes(8)) 
				attributeVals(9) = new AttributeValue(this.timestamp_v, attributeTypes(9)) 
				attributeVals(10) = new AttributeValue(this.currentserviceclass, attributeTypes(10)) 
				attributeVals(11) = new AttributeValue(this.voucherbasedrefill, attributeTypes(11)) 
				attributeVals(12) = new AttributeValue(this.transactiontype, attributeTypes(12)) 
				attributeVals(13) = new AttributeValue(this.transactioncode, attributeTypes(13)) 
				attributeVals(14) = new AttributeValue(this.transactionamount, attributeTypes(14)) 
				attributeVals(15) = new AttributeValue(this.transactioncurrency, attributeTypes(15)) 
				attributeVals(16) = new AttributeValue(this.refillamountconverted, attributeTypes(16)) 
				attributeVals(17) = new AttributeValue(this.refilldivisionamount, attributeTypes(17)) 
				attributeVals(18) = new AttributeValue(this.refilltype, attributeTypes(18)) 
				attributeVals(19) = new AttributeValue(this.refillprofileid, attributeTypes(19)) 
				attributeVals(20) = new AttributeValue(this.segmentationid, attributeTypes(20)) 
				attributeVals(21) = new AttributeValue(this.voucherserialnumber, attributeTypes(21)) 
				attributeVals(22) = new AttributeValue(this.vouchergroupid, attributeTypes(22)) 
				attributeVals(23) = new AttributeValue(this.accountnumber, attributeTypes(23)) 
				attributeVals(24) = new AttributeValue(this.accountcurrency, attributeTypes(24)) 
				attributeVals(25) = new AttributeValue(this.subscribernumber, attributeTypes(25)) 
				attributeVals(26) = new AttributeValue(this.promotionannouncementcode, attributeTypes(26)) 
				attributeVals(27) = new AttributeValue(this.accountflags, attributeTypes(27)) 
				attributeVals(28) = new AttributeValue(this.accountbalance, attributeTypes(28)) 
				attributeVals(29) = new AttributeValue(this.accumulatedrefillvalue, attributeTypes(29)) 
				attributeVals(30) = new AttributeValue(this.accumulatedrefillcounter, attributeTypes(30)) 
				attributeVals(31) = new AttributeValue(this.accumulatedprogressionvalue, attributeTypes(31)) 
				attributeVals(32) = new AttributeValue(this.accumulatedprogressioncounter, attributeTypes(32)) 
				attributeVals(33) = new AttributeValue(this.creditclearanceperiod, attributeTypes(33)) 
				attributeVals(34) = new AttributeValue(this.promotionplan, attributeTypes(34)) 
				attributeVals(35) = new AttributeValue(this.permanentserviceclass, attributeTypes(35)) 
				attributeVals(36) = new AttributeValue(this.temporaryserviceclass, attributeTypes(36)) 
				attributeVals(37) = new AttributeValue(this.temporaryserviceclassexpirydate, attributeTypes(37)) 
				attributeVals(38) = new AttributeValue(this.refilloption, attributeTypes(38)) 
				attributeVals(39) = new AttributeValue(this.servicefeeexpirydate, attributeTypes(39)) 
				attributeVals(40) = new AttributeValue(this.serviceremovalgraceperiod, attributeTypes(40)) 
				attributeVals(41) = new AttributeValue(this.serviceoffering, attributeTypes(41)) 
				attributeVals(42) = new AttributeValue(this.supervisionexpirydate, attributeTypes(42)) 
				attributeVals(43) = new AttributeValue(this.communityid1, attributeTypes(43)) 
				attributeVals(44) = new AttributeValue(this.communityid2, attributeTypes(44)) 
				attributeVals(45) = new AttributeValue(this.communityid3, attributeTypes(45)) 
				attributeVals(46) = new AttributeValue(this.offeridentifier, attributeTypes(46)) 
				attributeVals(47) = new AttributeValue(this.offerstartdate, attributeTypes(47)) 
				attributeVals(48) = new AttributeValue(this.offerexpirydate, attributeTypes(48)) 
				attributeVals(49) = new AttributeValue(this.offertype, attributeTypes(49)) 
				attributeVals(50) = new AttributeValue(this.offeridentifier____1, attributeTypes(50)) 
				attributeVals(51) = new AttributeValue(this.offerstartdate____1, attributeTypes(51)) 
				attributeVals(52) = new AttributeValue(this.offerexpirydate____1, attributeTypes(52)) 
				attributeVals(53) = new AttributeValue(this.offertype____1, attributeTypes(53)) 
				attributeVals(54) = new AttributeValue(this.offeridentifier____2, attributeTypes(54)) 
				attributeVals(55) = new AttributeValue(this.offerstartdate____2, attributeTypes(55)) 
				attributeVals(56) = new AttributeValue(this.offerexpirydate____2, attributeTypes(56)) 
				attributeVals(57) = new AttributeValue(this.offertype____2, attributeTypes(57)) 
				attributeVals(58) = new AttributeValue(this.offeridentifier____3, attributeTypes(58)) 
				attributeVals(59) = new AttributeValue(this.offerstartdate____3, attributeTypes(59)) 
				attributeVals(60) = new AttributeValue(this.offerexpirydate____3, attributeTypes(60)) 
				attributeVals(61) = new AttributeValue(this.offertype____3, attributeTypes(61)) 
				attributeVals(62) = new AttributeValue(this.offeridentifier____4, attributeTypes(62)) 
				attributeVals(63) = new AttributeValue(this.offerstartdate____4, attributeTypes(63)) 
				attributeVals(64) = new AttributeValue(this.offerexpirydate____4, attributeTypes(64)) 
				attributeVals(65) = new AttributeValue(this.offertype____4, attributeTypes(65)) 
				attributeVals(66) = new AttributeValue(this.accountflags____1, attributeTypes(66)) 
				attributeVals(67) = new AttributeValue(this.accountbalance____1, attributeTypes(67)) 
				attributeVals(68) = new AttributeValue(this.accumulatedrefillvalue____1, attributeTypes(68)) 
				attributeVals(69) = new AttributeValue(this.accumulatedrefillcounter____1, attributeTypes(69)) 
				attributeVals(70) = new AttributeValue(this.accumulatedprogressionvalue____1, attributeTypes(70)) 
				attributeVals(71) = new AttributeValue(this.accumulatedprogressioncounter____1, attributeTypes(71)) 
				attributeVals(72) = new AttributeValue(this.creditclearanceperiod____1, attributeTypes(72)) 
				attributeVals(73) = new AttributeValue(this.promotionplan____1, attributeTypes(73)) 
				attributeVals(74) = new AttributeValue(this.permanentserviceclass____1, attributeTypes(74)) 
				attributeVals(75) = new AttributeValue(this.temporaryserviceclass____1, attributeTypes(75)) 
				attributeVals(76) = new AttributeValue(this.temporaryserviceclassexpirydate____1, attributeTypes(76)) 
				attributeVals(77) = new AttributeValue(this.refilloption____1, attributeTypes(77)) 
				attributeVals(78) = new AttributeValue(this.servicefeeexpirydate____1, attributeTypes(78)) 
				attributeVals(79) = new AttributeValue(this.serviceremovalgraceperiod____1, attributeTypes(79)) 
				attributeVals(80) = new AttributeValue(this.serviceoffering____1, attributeTypes(80)) 
				attributeVals(81) = new AttributeValue(this.supervisionexpirydate____1, attributeTypes(81)) 
				attributeVals(82) = new AttributeValue(this.communityid1____1, attributeTypes(82)) 
				attributeVals(83) = new AttributeValue(this.communityid2____1, attributeTypes(83)) 
				attributeVals(84) = new AttributeValue(this.communityid3____1, attributeTypes(84)) 
				attributeVals(85) = new AttributeValue(this.offeridentifier____5, attributeTypes(85)) 
				attributeVals(86) = new AttributeValue(this.offerstartdate____5, attributeTypes(86)) 
				attributeVals(87) = new AttributeValue(this.offerexpirydate____5, attributeTypes(87)) 
				attributeVals(88) = new AttributeValue(this.offertype____5, attributeTypes(88)) 
				attributeVals(89) = new AttributeValue(this.offeridentifier____6, attributeTypes(89)) 
				attributeVals(90) = new AttributeValue(this.offerstartdate____6, attributeTypes(90)) 
				attributeVals(91) = new AttributeValue(this.offerexpirydate____6, attributeTypes(91)) 
				attributeVals(92) = new AttributeValue(this.offertype____6, attributeTypes(92)) 
				attributeVals(93) = new AttributeValue(this.offeridentifier____7, attributeTypes(93)) 
				attributeVals(94) = new AttributeValue(this.offerstartdate____7, attributeTypes(94)) 
				attributeVals(95) = new AttributeValue(this.offerexpirydate____7, attributeTypes(95)) 
				attributeVals(96) = new AttributeValue(this.offertype____7, attributeTypes(96)) 
				attributeVals(97) = new AttributeValue(this.offeridentifier____8, attributeTypes(97)) 
				attributeVals(98) = new AttributeValue(this.offerstartdate____8, attributeTypes(98)) 
				attributeVals(99) = new AttributeValue(this.offerexpirydate____8, attributeTypes(99)) 
				attributeVals(100) = new AttributeValue(this.offertype____8, attributeTypes(100)) 
				attributeVals(101) = new AttributeValue(this.offeridentifier____9, attributeTypes(101)) 
				attributeVals(102) = new AttributeValue(this.offerstartdate____9, attributeTypes(102)) 
				attributeVals(103) = new AttributeValue(this.offerexpirydate____9, attributeTypes(103)) 
				attributeVals(104) = new AttributeValue(this.offertype____9, attributeTypes(104)) 
				attributeVals(105) = new AttributeValue(this.refillpromodivisionamount, attributeTypes(105)) 
				attributeVals(106) = new AttributeValue(this.supervisiondayspromopart, attributeTypes(106)) 
				attributeVals(107) = new AttributeValue(this.supervisiondayssurplus, attributeTypes(107)) 
				attributeVals(108) = new AttributeValue(this.servicefeedayspromopart, attributeTypes(108)) 
				attributeVals(109) = new AttributeValue(this.servicefeedayssurplus, attributeTypes(109)) 
				attributeVals(110) = new AttributeValue(this.maximumservicefeeperiod, attributeTypes(110)) 
				attributeVals(111) = new AttributeValue(this.maximumsupervisionperiod, attributeTypes(111)) 
				attributeVals(112) = new AttributeValue(this.activationdate, attributeTypes(112)) 
				attributeVals(113) = new AttributeValue(this.welcomestatus, attributeTypes(113)) 
				attributeVals(114) = new AttributeValue(this.voucheragent, attributeTypes(114)) 
				attributeVals(115) = new AttributeValue(this.promotionplanallocstartdate, attributeTypes(115)) 
				attributeVals(116) = new AttributeValue(this.accountgroupid, attributeTypes(116)) 
				attributeVals(117) = new AttributeValue(this.externaldata1, attributeTypes(117)) 
				attributeVals(118) = new AttributeValue(this.externaldata2, attributeTypes(118)) 
				attributeVals(119) = new AttributeValue(this.externaldata3, attributeTypes(119)) 
				attributeVals(120) = new AttributeValue(this.externaldata4, attributeTypes(120)) 
				attributeVals(121) = new AttributeValue(this.locationnumber, attributeTypes(121)) 
				attributeVals(122) = new AttributeValue(this.voucheractivationcode, attributeTypes(122)) 
				attributeVals(123) = new AttributeValue(this.accountcurrencycleared, attributeTypes(123)) 
				attributeVals(124) = new AttributeValue(this.ignoreserviceclasshierarchy, attributeTypes(124)) 
				attributeVals(125) = new AttributeValue(this.selectiontreeid, attributeTypes(125)) 
				attributeVals(126) = new AttributeValue(this.selectiontreeversion, attributeTypes(126)) 
				attributeVals(127) = new AttributeValue(this.selectiontreeid____1, attributeTypes(127)) 
				attributeVals(128) = new AttributeValue(this.selectiontreeversion____1, attributeTypes(128)) 
				attributeVals(129) = new AttributeValue(this.selectiontreeid____2, attributeTypes(129)) 
				attributeVals(130) = new AttributeValue(this.selectiontreeversion____2, attributeTypes(130)) 
				attributeVals(131) = new AttributeValue(this.selectiontreeid____3, attributeTypes(131)) 
				attributeVals(132) = new AttributeValue(this.selectiontreeversion____3, attributeTypes(132)) 
				attributeVals(133) = new AttributeValue(this.selectiontreeid____4, attributeTypes(133)) 
				attributeVals(134) = new AttributeValue(this.selectiontreeversion____4, attributeTypes(134)) 
				attributeVals(135) = new AttributeValue(this.parameterid, attributeTypes(135)) 
				attributeVals(136) = new AttributeValue(this.parametervalue, attributeTypes(136)) 
				attributeVals(137) = new AttributeValue(this.parameterid____1, attributeTypes(137)) 
				attributeVals(138) = new AttributeValue(this.parametervalue____1, attributeTypes(138)) 
				attributeVals(139) = new AttributeValue(this.parameterid____2, attributeTypes(139)) 
				attributeVals(140) = new AttributeValue(this.parametervalue____2, attributeTypes(140)) 
				attributeVals(141) = new AttributeValue(this.parameterid____3, attributeTypes(141)) 
				attributeVals(142) = new AttributeValue(this.parametervalue____3, attributeTypes(142)) 
				attributeVals(143) = new AttributeValue(this.parameterid____4, attributeTypes(143)) 
				attributeVals(144) = new AttributeValue(this.parametervalue____4, attributeTypes(144)) 
				attributeVals(145) = new AttributeValue(this.accounthomeregion, attributeTypes(145)) 
				attributeVals(146) = new AttributeValue(this.subscriberregion, attributeTypes(146)) 
				attributeVals(147) = new AttributeValue(this.voucherregion, attributeTypes(147)) 
				attributeVals(148) = new AttributeValue(this.promotionplanallocenddate, attributeTypes(148)) 
				attributeVals(149) = new AttributeValue(this.requestedrefilltype, attributeTypes(149)) 
				attributeVals(150) = new AttributeValue(this.cellglobalid, attributeTypes(150)) 
				attributeVals(151) = new AttributeValue(this.dasizeafter, attributeTypes(151)) 
				attributeVals(152) = new AttributeValue(this.dasizebefore, attributeTypes(152)) 
				attributeVals(153) = new AttributeValue(this.accsizebefore, attributeTypes(153)) 
				attributeVals(154) = new AttributeValue(this.accsizeafter, attributeTypes(154)) 
				attributeVals(155) = new AttributeValue(this.serviceclassid, attributeTypes(155)) 
				attributeVals(156) = new AttributeValue(this.paytype, attributeTypes(156)) 
				attributeVals(157) = new AttributeValue(this.callreferenceid, attributeTypes(157)) 
				attributeVals(158) = new AttributeValue(this.mainamountused, attributeTypes(158)) 
				attributeVals(159) = new AttributeValue(this.file_name, attributeTypes(159)) 
				attributeVals(160) = new AttributeValue(this.file_offset, attributeTypes(160)) 
				attributeVals(161) = new AttributeValue(this.kamanja_loaded_date, attributeTypes(161)) 
				attributeVals(162) = new AttributeValue(this.file_mod_date, attributeTypes(162)) 
				attributeVals(163) = new AttributeValue(this.recharge_type_enrich, attributeTypes(163)) 
				attributeVals(164) = new AttributeValue(this.date_key, attributeTypes(164)) 
				attributeVals(165) = new AttributeValue(this.msisdn_key, attributeTypes(165)) 
				attributeVals(166) = new AttributeValue(this.event_timestamp_enrich, attributeTypes(166)) 
				attributeVals(167) = new AttributeValue(this.original_timestamp_enrich, attributeTypes(167)) 
				attributeVals(168) = new AttributeValue(this.transaction_amt, attributeTypes(168)) 
				attributeVals(169) = new AttributeValue(this.origin_time_stamp, attributeTypes(169)) 
				attributeVals(170) = new AttributeValue(this.voucher_serial_nr, attributeTypes(170)) 
				attributeVals(171) = new AttributeValue(this.origin_host_nm, attributeTypes(171)) 
				attributeVals(172) = new AttributeValue(this.external_data1, attributeTypes(172)) 
				attributeVals(173) = new AttributeValue(this.msg_unique_id_enrich, attributeTypes(173)) 
				attributeVals(174) = new AttributeValue(this.base_file_name, attributeTypes(174)) 
				attributeVals(175) = new AttributeValue(this.path, attributeTypes(175)) 
				attributeVals(176) = new AttributeValue(this.line_number, attributeTypes(176)) 
				attributeVals(177) = new AttributeValue(this.file_id, attributeTypes(177)) 
				attributeVals(178) = new AttributeValue(this.processed_timestamp, attributeTypes(178)) 
				attributeVals(179) = new AttributeValue(this.ltz_event_timestamp_enrich, attributeTypes(179)) 
				attributeVals(180) = new AttributeValue(this.kamanja_system_null_flags, attributeTypes(180)) 
       
      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return attributeVals;
    }      
    
    override def getOnlyValuesForAllAttributes(): Array[Object] = {
      var allVals = new Array[Object](181);
      try{
 				allVals(0) = this.edrtype.asInstanceOf[AnyRef]; 
				allVals(1) = this.originnodetype.asInstanceOf[AnyRef]; 
				allVals(2) = this.originhostname.asInstanceOf[AnyRef]; 
				allVals(3) = this.originfileid.asInstanceOf[AnyRef]; 
				allVals(4) = this.origintransactionid.asInstanceOf[AnyRef]; 
				allVals(5) = this.originoperatorid.asInstanceOf[AnyRef]; 
				allVals(6) = this.origintimestamp.asInstanceOf[AnyRef]; 
				allVals(7) = this.hostname.asInstanceOf[AnyRef]; 
				allVals(8) = this.localsequencenumber.asInstanceOf[AnyRef]; 
				allVals(9) = this.timestamp_v.asInstanceOf[AnyRef]; 
				allVals(10) = this.currentserviceclass.asInstanceOf[AnyRef]; 
				allVals(11) = this.voucherbasedrefill.asInstanceOf[AnyRef]; 
				allVals(12) = this.transactiontype.asInstanceOf[AnyRef]; 
				allVals(13) = this.transactioncode.asInstanceOf[AnyRef]; 
				allVals(14) = this.transactionamount.asInstanceOf[AnyRef]; 
				allVals(15) = this.transactioncurrency.asInstanceOf[AnyRef]; 
				allVals(16) = this.refillamountconverted.asInstanceOf[AnyRef]; 
				allVals(17) = this.refilldivisionamount.asInstanceOf[AnyRef]; 
				allVals(18) = this.refilltype.asInstanceOf[AnyRef]; 
				allVals(19) = this.refillprofileid.asInstanceOf[AnyRef]; 
				allVals(20) = this.segmentationid.asInstanceOf[AnyRef]; 
				allVals(21) = this.voucherserialnumber.asInstanceOf[AnyRef]; 
				allVals(22) = this.vouchergroupid.asInstanceOf[AnyRef]; 
				allVals(23) = this.accountnumber.asInstanceOf[AnyRef]; 
				allVals(24) = this.accountcurrency.asInstanceOf[AnyRef]; 
				allVals(25) = this.subscribernumber.asInstanceOf[AnyRef]; 
				allVals(26) = this.promotionannouncementcode.asInstanceOf[AnyRef]; 
				allVals(27) = this.accountflags.asInstanceOf[AnyRef]; 
				allVals(28) = this.accountbalance.asInstanceOf[AnyRef]; 
				allVals(29) = this.accumulatedrefillvalue.asInstanceOf[AnyRef]; 
				allVals(30) = this.accumulatedrefillcounter.asInstanceOf[AnyRef]; 
				allVals(31) = this.accumulatedprogressionvalue.asInstanceOf[AnyRef]; 
				allVals(32) = this.accumulatedprogressioncounter.asInstanceOf[AnyRef]; 
				allVals(33) = this.creditclearanceperiod.asInstanceOf[AnyRef]; 
				allVals(34) = this.promotionplan.asInstanceOf[AnyRef]; 
				allVals(35) = this.permanentserviceclass.asInstanceOf[AnyRef]; 
				allVals(36) = this.temporaryserviceclass.asInstanceOf[AnyRef]; 
				allVals(37) = this.temporaryserviceclassexpirydate.asInstanceOf[AnyRef]; 
				allVals(38) = this.refilloption.asInstanceOf[AnyRef]; 
				allVals(39) = this.servicefeeexpirydate.asInstanceOf[AnyRef]; 
				allVals(40) = this.serviceremovalgraceperiod.asInstanceOf[AnyRef]; 
				allVals(41) = this.serviceoffering.asInstanceOf[AnyRef]; 
				allVals(42) = this.supervisionexpirydate.asInstanceOf[AnyRef]; 
				allVals(43) = this.communityid1.asInstanceOf[AnyRef]; 
				allVals(44) = this.communityid2.asInstanceOf[AnyRef]; 
				allVals(45) = this.communityid3.asInstanceOf[AnyRef]; 
				allVals(46) = this.offeridentifier.asInstanceOf[AnyRef]; 
				allVals(47) = this.offerstartdate.asInstanceOf[AnyRef]; 
				allVals(48) = this.offerexpirydate.asInstanceOf[AnyRef]; 
				allVals(49) = this.offertype.asInstanceOf[AnyRef]; 
				allVals(50) = this.offeridentifier____1.asInstanceOf[AnyRef]; 
				allVals(51) = this.offerstartdate____1.asInstanceOf[AnyRef]; 
				allVals(52) = this.offerexpirydate____1.asInstanceOf[AnyRef]; 
				allVals(53) = this.offertype____1.asInstanceOf[AnyRef]; 
				allVals(54) = this.offeridentifier____2.asInstanceOf[AnyRef]; 
				allVals(55) = this.offerstartdate____2.asInstanceOf[AnyRef]; 
				allVals(56) = this.offerexpirydate____2.asInstanceOf[AnyRef]; 
				allVals(57) = this.offertype____2.asInstanceOf[AnyRef]; 
				allVals(58) = this.offeridentifier____3.asInstanceOf[AnyRef]; 
				allVals(59) = this.offerstartdate____3.asInstanceOf[AnyRef]; 
				allVals(60) = this.offerexpirydate____3.asInstanceOf[AnyRef]; 
				allVals(61) = this.offertype____3.asInstanceOf[AnyRef]; 
				allVals(62) = this.offeridentifier____4.asInstanceOf[AnyRef]; 
				allVals(63) = this.offerstartdate____4.asInstanceOf[AnyRef]; 
				allVals(64) = this.offerexpirydate____4.asInstanceOf[AnyRef]; 
				allVals(65) = this.offertype____4.asInstanceOf[AnyRef]; 
				allVals(66) = this.accountflags____1.asInstanceOf[AnyRef]; 
				allVals(67) = this.accountbalance____1.asInstanceOf[AnyRef]; 
				allVals(68) = this.accumulatedrefillvalue____1.asInstanceOf[AnyRef]; 
				allVals(69) = this.accumulatedrefillcounter____1.asInstanceOf[AnyRef]; 
				allVals(70) = this.accumulatedprogressionvalue____1.asInstanceOf[AnyRef]; 
				allVals(71) = this.accumulatedprogressioncounter____1.asInstanceOf[AnyRef]; 
				allVals(72) = this.creditclearanceperiod____1.asInstanceOf[AnyRef]; 
				allVals(73) = this.promotionplan____1.asInstanceOf[AnyRef]; 
				allVals(74) = this.permanentserviceclass____1.asInstanceOf[AnyRef]; 
				allVals(75) = this.temporaryserviceclass____1.asInstanceOf[AnyRef]; 
				allVals(76) = this.temporaryserviceclassexpirydate____1.asInstanceOf[AnyRef]; 
				allVals(77) = this.refilloption____1.asInstanceOf[AnyRef]; 
				allVals(78) = this.servicefeeexpirydate____1.asInstanceOf[AnyRef]; 
				allVals(79) = this.serviceremovalgraceperiod____1.asInstanceOf[AnyRef]; 
				allVals(80) = this.serviceoffering____1.asInstanceOf[AnyRef]; 
				allVals(81) = this.supervisionexpirydate____1.asInstanceOf[AnyRef]; 
				allVals(82) = this.communityid1____1.asInstanceOf[AnyRef]; 
				allVals(83) = this.communityid2____1.asInstanceOf[AnyRef]; 
				allVals(84) = this.communityid3____1.asInstanceOf[AnyRef]; 
				allVals(85) = this.offeridentifier____5.asInstanceOf[AnyRef]; 
				allVals(86) = this.offerstartdate____5.asInstanceOf[AnyRef]; 
				allVals(87) = this.offerexpirydate____5.asInstanceOf[AnyRef]; 
				allVals(88) = this.offertype____5.asInstanceOf[AnyRef]; 
				allVals(89) = this.offeridentifier____6.asInstanceOf[AnyRef]; 
				allVals(90) = this.offerstartdate____6.asInstanceOf[AnyRef]; 
				allVals(91) = this.offerexpirydate____6.asInstanceOf[AnyRef]; 
				allVals(92) = this.offertype____6.asInstanceOf[AnyRef]; 
				allVals(93) = this.offeridentifier____7.asInstanceOf[AnyRef]; 
				allVals(94) = this.offerstartdate____7.asInstanceOf[AnyRef]; 
				allVals(95) = this.offerexpirydate____7.asInstanceOf[AnyRef]; 
				allVals(96) = this.offertype____7.asInstanceOf[AnyRef]; 
				allVals(97) = this.offeridentifier____8.asInstanceOf[AnyRef]; 
				allVals(98) = this.offerstartdate____8.asInstanceOf[AnyRef]; 
				allVals(99) = this.offerexpirydate____8.asInstanceOf[AnyRef]; 
				allVals(100) = this.offertype____8.asInstanceOf[AnyRef]; 
				allVals(101) = this.offeridentifier____9.asInstanceOf[AnyRef]; 
				allVals(102) = this.offerstartdate____9.asInstanceOf[AnyRef]; 
				allVals(103) = this.offerexpirydate____9.asInstanceOf[AnyRef]; 
				allVals(104) = this.offertype____9.asInstanceOf[AnyRef]; 
				allVals(105) = this.refillpromodivisionamount.asInstanceOf[AnyRef]; 
				allVals(106) = this.supervisiondayspromopart.asInstanceOf[AnyRef]; 
				allVals(107) = this.supervisiondayssurplus.asInstanceOf[AnyRef]; 
				allVals(108) = this.servicefeedayspromopart.asInstanceOf[AnyRef]; 
				allVals(109) = this.servicefeedayssurplus.asInstanceOf[AnyRef]; 
				allVals(110) = this.maximumservicefeeperiod.asInstanceOf[AnyRef]; 
				allVals(111) = this.maximumsupervisionperiod.asInstanceOf[AnyRef]; 
				allVals(112) = this.activationdate.asInstanceOf[AnyRef]; 
				allVals(113) = this.welcomestatus.asInstanceOf[AnyRef]; 
				allVals(114) = this.voucheragent.asInstanceOf[AnyRef]; 
				allVals(115) = this.promotionplanallocstartdate.asInstanceOf[AnyRef]; 
				allVals(116) = this.accountgroupid.asInstanceOf[AnyRef]; 
				allVals(117) = this.externaldata1.asInstanceOf[AnyRef]; 
				allVals(118) = this.externaldata2.asInstanceOf[AnyRef]; 
				allVals(119) = this.externaldata3.asInstanceOf[AnyRef]; 
				allVals(120) = this.externaldata4.asInstanceOf[AnyRef]; 
				allVals(121) = this.locationnumber.asInstanceOf[AnyRef]; 
				allVals(122) = this.voucheractivationcode.asInstanceOf[AnyRef]; 
				allVals(123) = this.accountcurrencycleared.asInstanceOf[AnyRef]; 
				allVals(124) = this.ignoreserviceclasshierarchy.asInstanceOf[AnyRef]; 
				allVals(125) = this.selectiontreeid.asInstanceOf[AnyRef]; 
				allVals(126) = this.selectiontreeversion.asInstanceOf[AnyRef]; 
				allVals(127) = this.selectiontreeid____1.asInstanceOf[AnyRef]; 
				allVals(128) = this.selectiontreeversion____1.asInstanceOf[AnyRef]; 
				allVals(129) = this.selectiontreeid____2.asInstanceOf[AnyRef]; 
				allVals(130) = this.selectiontreeversion____2.asInstanceOf[AnyRef]; 
				allVals(131) = this.selectiontreeid____3.asInstanceOf[AnyRef]; 
				allVals(132) = this.selectiontreeversion____3.asInstanceOf[AnyRef]; 
				allVals(133) = this.selectiontreeid____4.asInstanceOf[AnyRef]; 
				allVals(134) = this.selectiontreeversion____4.asInstanceOf[AnyRef]; 
				allVals(135) = this.parameterid.asInstanceOf[AnyRef]; 
				allVals(136) = this.parametervalue.asInstanceOf[AnyRef]; 
				allVals(137) = this.parameterid____1.asInstanceOf[AnyRef]; 
				allVals(138) = this.parametervalue____1.asInstanceOf[AnyRef]; 
				allVals(139) = this.parameterid____2.asInstanceOf[AnyRef]; 
				allVals(140) = this.parametervalue____2.asInstanceOf[AnyRef]; 
				allVals(141) = this.parameterid____3.asInstanceOf[AnyRef]; 
				allVals(142) = this.parametervalue____3.asInstanceOf[AnyRef]; 
				allVals(143) = this.parameterid____4.asInstanceOf[AnyRef]; 
				allVals(144) = this.parametervalue____4.asInstanceOf[AnyRef]; 
				allVals(145) = this.accounthomeregion.asInstanceOf[AnyRef]; 
				allVals(146) = this.subscriberregion.asInstanceOf[AnyRef]; 
				allVals(147) = this.voucherregion.asInstanceOf[AnyRef]; 
				allVals(148) = this.promotionplanallocenddate.asInstanceOf[AnyRef]; 
				allVals(149) = this.requestedrefilltype.asInstanceOf[AnyRef]; 
				allVals(150) = this.cellglobalid.asInstanceOf[AnyRef]; 
				allVals(151) = this.dasizeafter.asInstanceOf[AnyRef]; 
				allVals(152) = this.dasizebefore.asInstanceOf[AnyRef]; 
				allVals(153) = this.accsizebefore.asInstanceOf[AnyRef]; 
				allVals(154) = this.accsizeafter.asInstanceOf[AnyRef]; 
				allVals(155) = this.serviceclassid.asInstanceOf[AnyRef]; 
				allVals(156) = this.paytype.asInstanceOf[AnyRef]; 
				allVals(157) = this.callreferenceid.asInstanceOf[AnyRef]; 
				allVals(158) = this.mainamountused.asInstanceOf[AnyRef]; 
				allVals(159) = this.file_name.asInstanceOf[AnyRef]; 
				allVals(160) = this.file_offset.asInstanceOf[AnyRef]; 
				allVals(161) = this.kamanja_loaded_date.asInstanceOf[AnyRef]; 
				allVals(162) = this.file_mod_date.asInstanceOf[AnyRef]; 
				allVals(163) = this.recharge_type_enrich.asInstanceOf[AnyRef]; 
				allVals(164) = this.date_key.asInstanceOf[AnyRef]; 
				allVals(165) = this.msisdn_key.asInstanceOf[AnyRef]; 
				allVals(166) = this.event_timestamp_enrich.asInstanceOf[AnyRef]; 
				allVals(167) = this.original_timestamp_enrich.asInstanceOf[AnyRef]; 
				allVals(168) = this.transaction_amt.asInstanceOf[AnyRef]; 
				allVals(169) = this.origin_time_stamp.asInstanceOf[AnyRef]; 
				allVals(170) = this.voucher_serial_nr.asInstanceOf[AnyRef]; 
				allVals(171) = this.origin_host_nm.asInstanceOf[AnyRef]; 
				allVals(172) = this.external_data1.asInstanceOf[AnyRef]; 
				allVals(173) = this.msg_unique_id_enrich.asInstanceOf[AnyRef]; 
				allVals(174) = this.base_file_name.asInstanceOf[AnyRef]; 
				allVals(175) = this.path.asInstanceOf[AnyRef]; 
				allVals(176) = this.line_number.asInstanceOf[AnyRef]; 
				allVals(177) = this.file_id.asInstanceOf[AnyRef]; 
				allVals(178) = this.processed_timestamp.asInstanceOf[AnyRef]; 
				allVals(179) = this.ltz_event_timestamp_enrich.asInstanceOf[AnyRef]; 
				allVals(180) = this.kamanja_system_null_flags.asInstanceOf[AnyRef]; 

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
   
  			 if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message CS5_AIR_REFILL_MA", null)
			 set(keyTypes(key).getIndex, value); 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
    }
  
      
    def set(index : Int, value :Any): Unit = {
      if (value == null) throw new Exception(s"Value is null for index $index in message CS5_AIR_REFILL_MA ")
      if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message CS5_AIR_REFILL_MA ")
      setFnArr(index)(this, value)
    }
    
    override def set(key: String, value: Any, valTyp: String) = {
      throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
    }
  
    private def fromFunc(other: CS5_AIR_REFILL_MA): CS5_AIR_REFILL_MA = {  
   			this.edrtype = com.ligadata.BaseTypes.StringImpl.Clone(other.edrtype);
			this.originnodetype = com.ligadata.BaseTypes.StringImpl.Clone(other.originnodetype);
			this.originhostname = com.ligadata.BaseTypes.StringImpl.Clone(other.originhostname);
			this.originfileid = com.ligadata.BaseTypes.StringImpl.Clone(other.originfileid);
			this.origintransactionid = com.ligadata.BaseTypes.StringImpl.Clone(other.origintransactionid);
			this.originoperatorid = com.ligadata.BaseTypes.StringImpl.Clone(other.originoperatorid);
			this.origintimestamp = com.ligadata.BaseTypes.StringImpl.Clone(other.origintimestamp);
			this.hostname = com.ligadata.BaseTypes.StringImpl.Clone(other.hostname);
			this.localsequencenumber = com.ligadata.BaseTypes.IntImpl.Clone(other.localsequencenumber);
			this.timestamp_v = com.ligadata.BaseTypes.LongImpl.Clone(other.timestamp_v);
			this.currentserviceclass = com.ligadata.BaseTypes.IntImpl.Clone(other.currentserviceclass);
			this.voucherbasedrefill = com.ligadata.BaseTypes.StringImpl.Clone(other.voucherbasedrefill);
			this.transactiontype = com.ligadata.BaseTypes.StringImpl.Clone(other.transactiontype);
			this.transactioncode = com.ligadata.BaseTypes.StringImpl.Clone(other.transactioncode);
			this.transactionamount = com.ligadata.BaseTypes.StringImpl.Clone(other.transactionamount);
			this.transactioncurrency = com.ligadata.BaseTypes.StringImpl.Clone(other.transactioncurrency);
			this.refillamountconverted = com.ligadata.BaseTypes.StringImpl.Clone(other.refillamountconverted);
			this.refilldivisionamount = com.ligadata.BaseTypes.StringImpl.Clone(other.refilldivisionamount);
			this.refilltype = com.ligadata.BaseTypes.IntImpl.Clone(other.refilltype);
			this.refillprofileid = com.ligadata.BaseTypes.StringImpl.Clone(other.refillprofileid);
			this.segmentationid = com.ligadata.BaseTypes.StringImpl.Clone(other.segmentationid);
			this.voucherserialnumber = com.ligadata.BaseTypes.StringImpl.Clone(other.voucherserialnumber);
			this.vouchergroupid = com.ligadata.BaseTypes.StringImpl.Clone(other.vouchergroupid);
			this.accountnumber = com.ligadata.BaseTypes.StringImpl.Clone(other.accountnumber);
			this.accountcurrency = com.ligadata.BaseTypes.StringImpl.Clone(other.accountcurrency);
			this.subscribernumber = com.ligadata.BaseTypes.StringImpl.Clone(other.subscribernumber);
			this.promotionannouncementcode = com.ligadata.BaseTypes.IntImpl.Clone(other.promotionannouncementcode);
			this.accountflags = com.ligadata.BaseTypes.StringImpl.Clone(other.accountflags);
			this.accountbalance = com.ligadata.BaseTypes.StringImpl.Clone(other.accountbalance);
			this.accumulatedrefillvalue = com.ligadata.BaseTypes.StringImpl.Clone(other.accumulatedrefillvalue);
			this.accumulatedrefillcounter = com.ligadata.BaseTypes.IntImpl.Clone(other.accumulatedrefillcounter);
			this.accumulatedprogressionvalue = com.ligadata.BaseTypes.StringImpl.Clone(other.accumulatedprogressionvalue);
			this.accumulatedprogressioncounter = com.ligadata.BaseTypes.IntImpl.Clone(other.accumulatedprogressioncounter);
			this.creditclearanceperiod = com.ligadata.BaseTypes.IntImpl.Clone(other.creditclearanceperiod);
			this.promotionplan = com.ligadata.BaseTypes.StringImpl.Clone(other.promotionplan);
			this.permanentserviceclass = com.ligadata.BaseTypes.IntImpl.Clone(other.permanentserviceclass);
			this.temporaryserviceclass = com.ligadata.BaseTypes.IntImpl.Clone(other.temporaryserviceclass);
			this.temporaryserviceclassexpirydate = com.ligadata.BaseTypes.StringImpl.Clone(other.temporaryserviceclassexpirydate);
			this.refilloption = com.ligadata.BaseTypes.IntImpl.Clone(other.refilloption);
			this.servicefeeexpirydate = com.ligadata.BaseTypes.StringImpl.Clone(other.servicefeeexpirydate);
			this.serviceremovalgraceperiod = com.ligadata.BaseTypes.IntImpl.Clone(other.serviceremovalgraceperiod);
			this.serviceoffering = com.ligadata.BaseTypes.IntImpl.Clone(other.serviceoffering);
			this.supervisionexpirydate = com.ligadata.BaseTypes.StringImpl.Clone(other.supervisionexpirydate);
			this.communityid1 = com.ligadata.BaseTypes.IntImpl.Clone(other.communityid1);
			this.communityid2 = com.ligadata.BaseTypes.IntImpl.Clone(other.communityid2);
			this.communityid3 = com.ligadata.BaseTypes.IntImpl.Clone(other.communityid3);
			this.offeridentifier = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier);
			this.offerstartdate = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate);
			this.offerexpirydate = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate);
			this.offertype = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype);
			this.offeridentifier____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____1);
			this.offerstartdate____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____1);
			this.offerexpirydate____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____1);
			this.offertype____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____1);
			this.offeridentifier____2 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____2);
			this.offerstartdate____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____2);
			this.offerexpirydate____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____2);
			this.offertype____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____2);
			this.offeridentifier____3 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____3);
			this.offerstartdate____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____3);
			this.offerexpirydate____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____3);
			this.offertype____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____3);
			this.offeridentifier____4 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____4);
			this.offerstartdate____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____4);
			this.offerexpirydate____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____4);
			this.offertype____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____4);
			this.accountflags____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.accountflags____1);
			this.accountbalance____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.accountbalance____1);
			this.accumulatedrefillvalue____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.accumulatedrefillvalue____1);
			this.accumulatedrefillcounter____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.accumulatedrefillcounter____1);
			this.accumulatedprogressionvalue____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.accumulatedprogressionvalue____1);
			this.accumulatedprogressioncounter____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.accumulatedprogressioncounter____1);
			this.creditclearanceperiod____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.creditclearanceperiod____1);
			this.promotionplan____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.promotionplan____1);
			this.permanentserviceclass____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.permanentserviceclass____1);
			this.temporaryserviceclass____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.temporaryserviceclass____1);
			this.temporaryserviceclassexpirydate____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.temporaryserviceclassexpirydate____1);
			this.refilloption____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.refilloption____1);
			this.servicefeeexpirydate____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.servicefeeexpirydate____1);
			this.serviceremovalgraceperiod____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.serviceremovalgraceperiod____1);
			this.serviceoffering____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.serviceoffering____1);
			this.supervisionexpirydate____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.supervisionexpirydate____1);
			this.communityid1____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.communityid1____1);
			this.communityid2____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.communityid2____1);
			this.communityid3____1 = com.ligadata.BaseTypes.IntImpl.Clone(other.communityid3____1);
			this.offeridentifier____5 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____5);
			this.offerstartdate____5 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____5);
			this.offerexpirydate____5 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____5);
			this.offertype____5 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____5);
			this.offeridentifier____6 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____6);
			this.offerstartdate____6 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____6);
			this.offerexpirydate____6 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____6);
			this.offertype____6 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____6);
			this.offeridentifier____7 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____7);
			this.offerstartdate____7 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____7);
			this.offerexpirydate____7 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____7);
			this.offertype____7 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____7);
			this.offeridentifier____8 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____8);
			this.offerstartdate____8 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____8);
			this.offerexpirydate____8 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____8);
			this.offertype____8 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____8);
			this.offeridentifier____9 = com.ligadata.BaseTypes.IntImpl.Clone(other.offeridentifier____9);
			this.offerstartdate____9 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerstartdate____9);
			this.offerexpirydate____9 = com.ligadata.BaseTypes.StringImpl.Clone(other.offerexpirydate____9);
			this.offertype____9 = com.ligadata.BaseTypes.StringImpl.Clone(other.offertype____9);
			this.refillpromodivisionamount = com.ligadata.BaseTypes.StringImpl.Clone(other.refillpromodivisionamount);
			this.supervisiondayspromopart = com.ligadata.BaseTypes.IntImpl.Clone(other.supervisiondayspromopart);
			this.supervisiondayssurplus = com.ligadata.BaseTypes.IntImpl.Clone(other.supervisiondayssurplus);
			this.servicefeedayspromopart = com.ligadata.BaseTypes.IntImpl.Clone(other.servicefeedayspromopart);
			this.servicefeedayssurplus = com.ligadata.BaseTypes.IntImpl.Clone(other.servicefeedayssurplus);
			this.maximumservicefeeperiod = com.ligadata.BaseTypes.IntImpl.Clone(other.maximumservicefeeperiod);
			this.maximumsupervisionperiod = com.ligadata.BaseTypes.IntImpl.Clone(other.maximumsupervisionperiod);
			this.activationdate = com.ligadata.BaseTypes.StringImpl.Clone(other.activationdate);
			this.welcomestatus = com.ligadata.BaseTypes.StringImpl.Clone(other.welcomestatus);
			this.voucheragent = com.ligadata.BaseTypes.StringImpl.Clone(other.voucheragent);
			this.promotionplanallocstartdate = com.ligadata.BaseTypes.StringImpl.Clone(other.promotionplanallocstartdate);
			this.accountgroupid = com.ligadata.BaseTypes.IntImpl.Clone(other.accountgroupid);
			this.externaldata1 = com.ligadata.BaseTypes.StringImpl.Clone(other.externaldata1);
			this.externaldata2 = com.ligadata.BaseTypes.StringImpl.Clone(other.externaldata2);
			this.externaldata3 = com.ligadata.BaseTypes.StringImpl.Clone(other.externaldata3);
			this.externaldata4 = com.ligadata.BaseTypes.StringImpl.Clone(other.externaldata4);
			this.locationnumber = com.ligadata.BaseTypes.StringImpl.Clone(other.locationnumber);
			this.voucheractivationcode = com.ligadata.BaseTypes.StringImpl.Clone(other.voucheractivationcode);
			this.accountcurrencycleared = com.ligadata.BaseTypes.StringImpl.Clone(other.accountcurrencycleared);
			this.ignoreserviceclasshierarchy = com.ligadata.BaseTypes.StringImpl.Clone(other.ignoreserviceclasshierarchy);
			this.selectiontreeid = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeid);
			this.selectiontreeversion = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeversion);
			this.selectiontreeid____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeid____1);
			this.selectiontreeversion____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeversion____1);
			this.selectiontreeid____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeid____2);
			this.selectiontreeversion____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeversion____2);
			this.selectiontreeid____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeid____3);
			this.selectiontreeversion____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeversion____3);
			this.selectiontreeid____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeid____4);
			this.selectiontreeversion____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.selectiontreeversion____4);
			this.parameterid = com.ligadata.BaseTypes.StringImpl.Clone(other.parameterid);
			this.parametervalue = com.ligadata.BaseTypes.StringImpl.Clone(other.parametervalue);
			this.parameterid____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.parameterid____1);
			this.parametervalue____1 = com.ligadata.BaseTypes.StringImpl.Clone(other.parametervalue____1);
			this.parameterid____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.parameterid____2);
			this.parametervalue____2 = com.ligadata.BaseTypes.StringImpl.Clone(other.parametervalue____2);
			this.parameterid____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.parameterid____3);
			this.parametervalue____3 = com.ligadata.BaseTypes.StringImpl.Clone(other.parametervalue____3);
			this.parameterid____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.parameterid____4);
			this.parametervalue____4 = com.ligadata.BaseTypes.StringImpl.Clone(other.parametervalue____4);
			this.accounthomeregion = com.ligadata.BaseTypes.IntImpl.Clone(other.accounthomeregion);
			this.subscriberregion = com.ligadata.BaseTypes.IntImpl.Clone(other.subscriberregion);
			this.voucherregion = com.ligadata.BaseTypes.IntImpl.Clone(other.voucherregion);
			this.promotionplanallocenddate = com.ligadata.BaseTypes.StringImpl.Clone(other.promotionplanallocenddate);
			this.requestedrefilltype = com.ligadata.BaseTypes.IntImpl.Clone(other.requestedrefilltype);
			this.cellglobalid = com.ligadata.BaseTypes.StringImpl.Clone(other.cellglobalid);
			this.dasizeafter = com.ligadata.BaseTypes.StringImpl.Clone(other.dasizeafter);
			this.dasizebefore = com.ligadata.BaseTypes.StringImpl.Clone(other.dasizebefore);
			this.accsizebefore = com.ligadata.BaseTypes.StringImpl.Clone(other.accsizebefore);
			this.accsizeafter = com.ligadata.BaseTypes.StringImpl.Clone(other.accsizeafter);
			this.serviceclassid = com.ligadata.BaseTypes.StringImpl.Clone(other.serviceclassid);
			this.paytype = com.ligadata.BaseTypes.StringImpl.Clone(other.paytype);
			this.callreferenceid = com.ligadata.BaseTypes.StringImpl.Clone(other.callreferenceid);
			this.mainamountused = com.ligadata.BaseTypes.StringImpl.Clone(other.mainamountused);
			this.file_name = com.ligadata.BaseTypes.StringImpl.Clone(other.file_name);
			this.file_offset = com.ligadata.BaseTypes.LongImpl.Clone(other.file_offset);
			this.kamanja_loaded_date = com.ligadata.BaseTypes.StringImpl.Clone(other.kamanja_loaded_date);
			this.file_mod_date = com.ligadata.BaseTypes.StringImpl.Clone(other.file_mod_date);
			this.recharge_type_enrich = com.ligadata.BaseTypes.IntImpl.Clone(other.recharge_type_enrich);
			this.date_key = com.ligadata.BaseTypes.IntImpl.Clone(other.date_key);
			this.msisdn_key = com.ligadata.BaseTypes.LongImpl.Clone(other.msisdn_key);
			this.event_timestamp_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.event_timestamp_enrich);
			this.original_timestamp_enrich = com.ligadata.BaseTypes.StringImpl.Clone(other.original_timestamp_enrich);
			this.transaction_amt = com.ligadata.BaseTypes.DoubleImpl.Clone(other.transaction_amt);
			this.origin_time_stamp = com.ligadata.BaseTypes.StringImpl.Clone(other.origin_time_stamp);
			this.voucher_serial_nr = com.ligadata.BaseTypes.StringImpl.Clone(other.voucher_serial_nr);
			this.origin_host_nm = com.ligadata.BaseTypes.StringImpl.Clone(other.origin_host_nm);
			this.external_data1 = com.ligadata.BaseTypes.StringImpl.Clone(other.external_data1);
			this.msg_unique_id_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.msg_unique_id_enrich);
			this.base_file_name = com.ligadata.BaseTypes.StringImpl.Clone(other.base_file_name);
			this.path = com.ligadata.BaseTypes.StringImpl.Clone(other.path);
			this.line_number = com.ligadata.BaseTypes.LongImpl.Clone(other.line_number);
			this.file_id = com.ligadata.BaseTypes.StringImpl.Clone(other.file_id);
			this.processed_timestamp = com.ligadata.BaseTypes.LongImpl.Clone(other.processed_timestamp);
			this.ltz_event_timestamp_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.ltz_event_timestamp_enrich);
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
    
    val uniqKeyColVals = Array[Any](edrtype,originnodetype,originhostname,originfileid,origintransactionid,originoperatorid,origintimestamp,hostname,localsequencenumber,timestamp_v,currentserviceclass,voucherbasedrefill,transactiontype,transactioncode,transactionamount,transactioncurrency,refillamountconverted,refilldivisionamount,refilltype,refillprofileid,segmentationid,voucherserialnumber,vouchergroupid,accountnumber,accountcurrency,subscribernumber,promotionannouncementcode,accountflags,accountbalance,accumulatedrefillvalue,accumulatedrefillcounter,accumulatedprogressionvalue,accumulatedprogressioncounter,creditclearanceperiod,promotionplan,permanentserviceclass,temporaryserviceclass,temporaryserviceclassexpirydate,refilloption,servicefeeexpirydate,serviceremovalgraceperiod,serviceoffering,supervisionexpirydate,communityid1,communityid2,communityid3,offeridentifier,offerstartdate,offerexpirydate,offertype,offeridentifier____1,offerstartdate____1,offerexpirydate____1,offertype____1,offeridentifier____2,offerstartdate____2,offerexpirydate____2,offertype____2,offeridentifier____3,offerstartdate____3,offerexpirydate____3,offertype____3,offeridentifier____4,offerstartdate____4,offerexpirydate____4,offertype____4,accountflags____1,accountbalance____1,accumulatedrefillvalue____1,accumulatedrefillcounter____1,accumulatedprogressionvalue____1,accumulatedprogressioncounter____1,creditclearanceperiod____1,promotionplan____1,permanentserviceclass____1,temporaryserviceclass____1,temporaryserviceclassexpirydate____1,refilloption____1,servicefeeexpirydate____1,serviceremovalgraceperiod____1,serviceoffering____1,supervisionexpirydate____1,communityid1____1,communityid2____1,communityid3____1,offeridentifier____5,offerstartdate____5,offerexpirydate____5,offertype____5,offeridentifier____6,offerstartdate____6,offerexpirydate____6,offertype____6,offeridentifier____7,offerstartdate____7,offerexpirydate____7,offertype____7,offeridentifier____8,offerstartdate____8,offerexpirydate____8,offertype____8,offeridentifier____9,offerstartdate____9,offerexpirydate____9,offertype____9,refillpromodivisionamount,supervisiondayspromopart,supervisiondayssurplus,servicefeedayspromopart,servicefeedayssurplus,maximumservicefeeperiod,maximumsupervisionperiod,activationdate,welcomestatus,voucheragent,promotionplanallocstartdate,accountgroupid,externaldata1,externaldata2,externaldata3,externaldata4,locationnumber,voucheractivationcode,accountcurrencycleared,ignoreserviceclasshierarchy,selectiontreeid,selectiontreeversion,selectiontreeid____1,selectiontreeversion____1,selectiontreeid____2,selectiontreeversion____2,selectiontreeid____3,selectiontreeversion____3,selectiontreeid____4,selectiontreeversion____4,parameterid,parametervalue,parameterid____1,parametervalue____1,parameterid____2,parametervalue____2,parameterid____3,parametervalue____3,parameterid____4,parametervalue____4,accounthomeregion,subscriberregion,voucherregion,promotionplanallocenddate,requestedrefilltype,cellglobalid,dasizeafter,dasizebefore,accsizebefore,accsizeafter,serviceclassid,paytype,callreferenceid,mainamountused)
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
 	 def withedrtype(value: String) : CS5_AIR_REFILL_MA = {
		 this.edrtype = value 
		 return this 
 	 } 
	 def withoriginnodetype(value: String) : CS5_AIR_REFILL_MA = {
		 this.originnodetype = value 
		 return this 
 	 } 
	 def withoriginhostname(value: String) : CS5_AIR_REFILL_MA = {
		 this.originhostname = value 
		 return this 
 	 } 
	 def withoriginfileid(value: String) : CS5_AIR_REFILL_MA = {
		 this.originfileid = value 
		 return this 
 	 } 
	 def withorigintransactionid(value: String) : CS5_AIR_REFILL_MA = {
		 this.origintransactionid = value 
		 return this 
 	 } 
	 def withoriginoperatorid(value: String) : CS5_AIR_REFILL_MA = {
		 this.originoperatorid = value 
		 return this 
 	 } 
	 def withorigintimestamp(value: String) : CS5_AIR_REFILL_MA = {
		 this.origintimestamp = value 
		 return this 
 	 } 
	 def withhostname(value: String) : CS5_AIR_REFILL_MA = {
		 this.hostname = value 
		 return this 
 	 } 
	 def withlocalsequencenumber(value: Int) : CS5_AIR_REFILL_MA = {
		 this.localsequencenumber = value 
		 return this 
 	 } 
	 def withtimestamp_v(value: Long) : CS5_AIR_REFILL_MA = {
		 this.timestamp_v = value 
		 return this 
 	 } 
	 def withcurrentserviceclass(value: Int) : CS5_AIR_REFILL_MA = {
		 this.currentserviceclass = value 
		 return this 
 	 } 
	 def withvoucherbasedrefill(value: String) : CS5_AIR_REFILL_MA = {
		 this.voucherbasedrefill = value 
		 return this 
 	 } 
	 def withtransactiontype(value: String) : CS5_AIR_REFILL_MA = {
		 this.transactiontype = value 
		 return this 
 	 } 
	 def withtransactioncode(value: String) : CS5_AIR_REFILL_MA = {
		 this.transactioncode = value 
		 return this 
 	 } 
	 def withtransactionamount(value: String) : CS5_AIR_REFILL_MA = {
		 this.transactionamount = value 
		 return this 
 	 } 
	 def withtransactioncurrency(value: String) : CS5_AIR_REFILL_MA = {
		 this.transactioncurrency = value 
		 return this 
 	 } 
	 def withrefillamountconverted(value: String) : CS5_AIR_REFILL_MA = {
		 this.refillamountconverted = value 
		 return this 
 	 } 
	 def withrefilldivisionamount(value: String) : CS5_AIR_REFILL_MA = {
		 this.refilldivisionamount = value 
		 return this 
 	 } 
	 def withrefilltype(value: Int) : CS5_AIR_REFILL_MA = {
		 this.refilltype = value 
		 return this 
 	 } 
	 def withrefillprofileid(value: String) : CS5_AIR_REFILL_MA = {
		 this.refillprofileid = value 
		 return this 
 	 } 
	 def withsegmentationid(value: String) : CS5_AIR_REFILL_MA = {
		 this.segmentationid = value 
		 return this 
 	 } 
	 def withvoucherserialnumber(value: String) : CS5_AIR_REFILL_MA = {
		 this.voucherserialnumber = value 
		 return this 
 	 } 
	 def withvouchergroupid(value: String) : CS5_AIR_REFILL_MA = {
		 this.vouchergroupid = value 
		 return this 
 	 } 
	 def withaccountnumber(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountnumber = value 
		 return this 
 	 } 
	 def withaccountcurrency(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountcurrency = value 
		 return this 
 	 } 
	 def withsubscribernumber(value: String) : CS5_AIR_REFILL_MA = {
		 this.subscribernumber = value 
		 return this 
 	 } 
	 def withpromotionannouncementcode(value: Int) : CS5_AIR_REFILL_MA = {
		 this.promotionannouncementcode = value 
		 return this 
 	 } 
	 def withaccountflags(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountflags = value 
		 return this 
 	 } 
	 def withaccountbalance(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountbalance = value 
		 return this 
 	 } 
	 def withaccumulatedrefillvalue(value: String) : CS5_AIR_REFILL_MA = {
		 this.accumulatedrefillvalue = value 
		 return this 
 	 } 
	 def withaccumulatedrefillcounter(value: Int) : CS5_AIR_REFILL_MA = {
		 this.accumulatedrefillcounter = value 
		 return this 
 	 } 
	 def withaccumulatedprogressionvalue(value: String) : CS5_AIR_REFILL_MA = {
		 this.accumulatedprogressionvalue = value 
		 return this 
 	 } 
	 def withaccumulatedprogressioncounter(value: Int) : CS5_AIR_REFILL_MA = {
		 this.accumulatedprogressioncounter = value 
		 return this 
 	 } 
	 def withcreditclearanceperiod(value: Int) : CS5_AIR_REFILL_MA = {
		 this.creditclearanceperiod = value 
		 return this 
 	 } 
	 def withpromotionplan(value: String) : CS5_AIR_REFILL_MA = {
		 this.promotionplan = value 
		 return this 
 	 } 
	 def withpermanentserviceclass(value: Int) : CS5_AIR_REFILL_MA = {
		 this.permanentserviceclass = value 
		 return this 
 	 } 
	 def withtemporaryserviceclass(value: Int) : CS5_AIR_REFILL_MA = {
		 this.temporaryserviceclass = value 
		 return this 
 	 } 
	 def withtemporaryserviceclassexpirydate(value: String) : CS5_AIR_REFILL_MA = {
		 this.temporaryserviceclassexpirydate = value 
		 return this 
 	 } 
	 def withrefilloption(value: Int) : CS5_AIR_REFILL_MA = {
		 this.refilloption = value 
		 return this 
 	 } 
	 def withservicefeeexpirydate(value: String) : CS5_AIR_REFILL_MA = {
		 this.servicefeeexpirydate = value 
		 return this 
 	 } 
	 def withserviceremovalgraceperiod(value: Int) : CS5_AIR_REFILL_MA = {
		 this.serviceremovalgraceperiod = value 
		 return this 
 	 } 
	 def withserviceoffering(value: Int) : CS5_AIR_REFILL_MA = {
		 this.serviceoffering = value 
		 return this 
 	 } 
	 def withsupervisionexpirydate(value: String) : CS5_AIR_REFILL_MA = {
		 this.supervisionexpirydate = value 
		 return this 
 	 } 
	 def withcommunityid1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.communityid1 = value 
		 return this 
 	 } 
	 def withcommunityid2(value: Int) : CS5_AIR_REFILL_MA = {
		 this.communityid2 = value 
		 return this 
 	 } 
	 def withcommunityid3(value: Int) : CS5_AIR_REFILL_MA = {
		 this.communityid3 = value 
		 return this 
 	 } 
	 def withofferidentifier(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier = value 
		 return this 
 	 } 
	 def withofferstartdate(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate = value 
		 return this 
 	 } 
	 def withofferexpirydate(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate = value 
		 return this 
 	 } 
	 def withoffertype(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype = value 
		 return this 
 	 } 
	 def withofferidentifier____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____1 = value 
		 return this 
 	 } 
	 def withofferstartdate____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____1 = value 
		 return this 
 	 } 
	 def withofferexpirydate____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____1 = value 
		 return this 
 	 } 
	 def withoffertype____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____1 = value 
		 return this 
 	 } 
	 def withofferidentifier____2(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____2 = value 
		 return this 
 	 } 
	 def withofferstartdate____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____2 = value 
		 return this 
 	 } 
	 def withofferexpirydate____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____2 = value 
		 return this 
 	 } 
	 def withoffertype____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____2 = value 
		 return this 
 	 } 
	 def withofferidentifier____3(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____3 = value 
		 return this 
 	 } 
	 def withofferstartdate____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____3 = value 
		 return this 
 	 } 
	 def withofferexpirydate____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____3 = value 
		 return this 
 	 } 
	 def withoffertype____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____3 = value 
		 return this 
 	 } 
	 def withofferidentifier____4(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____4 = value 
		 return this 
 	 } 
	 def withofferstartdate____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____4 = value 
		 return this 
 	 } 
	 def withofferexpirydate____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____4 = value 
		 return this 
 	 } 
	 def withoffertype____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____4 = value 
		 return this 
 	 } 
	 def withaccountflags____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountflags____1 = value 
		 return this 
 	 } 
	 def withaccountbalance____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountbalance____1 = value 
		 return this 
 	 } 
	 def withaccumulatedrefillvalue____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.accumulatedrefillvalue____1 = value 
		 return this 
 	 } 
	 def withaccumulatedrefillcounter____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.accumulatedrefillcounter____1 = value 
		 return this 
 	 } 
	 def withaccumulatedprogressionvalue____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.accumulatedprogressionvalue____1 = value 
		 return this 
 	 } 
	 def withaccumulatedprogressioncounter____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.accumulatedprogressioncounter____1 = value 
		 return this 
 	 } 
	 def withcreditclearanceperiod____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.creditclearanceperiod____1 = value 
		 return this 
 	 } 
	 def withpromotionplan____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.promotionplan____1 = value 
		 return this 
 	 } 
	 def withpermanentserviceclass____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.permanentserviceclass____1 = value 
		 return this 
 	 } 
	 def withtemporaryserviceclass____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.temporaryserviceclass____1 = value 
		 return this 
 	 } 
	 def withtemporaryserviceclassexpirydate____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.temporaryserviceclassexpirydate____1 = value 
		 return this 
 	 } 
	 def withrefilloption____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.refilloption____1 = value 
		 return this 
 	 } 
	 def withservicefeeexpirydate____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.servicefeeexpirydate____1 = value 
		 return this 
 	 } 
	 def withserviceremovalgraceperiod____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.serviceremovalgraceperiod____1 = value 
		 return this 
 	 } 
	 def withserviceoffering____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.serviceoffering____1 = value 
		 return this 
 	 } 
	 def withsupervisionexpirydate____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.supervisionexpirydate____1 = value 
		 return this 
 	 } 
	 def withcommunityid1____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.communityid1____1 = value 
		 return this 
 	 } 
	 def withcommunityid2____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.communityid2____1 = value 
		 return this 
 	 } 
	 def withcommunityid3____1(value: Int) : CS5_AIR_REFILL_MA = {
		 this.communityid3____1 = value 
		 return this 
 	 } 
	 def withofferidentifier____5(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____5 = value 
		 return this 
 	 } 
	 def withofferstartdate____5(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____5 = value 
		 return this 
 	 } 
	 def withofferexpirydate____5(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____5 = value 
		 return this 
 	 } 
	 def withoffertype____5(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____5 = value 
		 return this 
 	 } 
	 def withofferidentifier____6(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____6 = value 
		 return this 
 	 } 
	 def withofferstartdate____6(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____6 = value 
		 return this 
 	 } 
	 def withofferexpirydate____6(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____6 = value 
		 return this 
 	 } 
	 def withoffertype____6(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____6 = value 
		 return this 
 	 } 
	 def withofferidentifier____7(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____7 = value 
		 return this 
 	 } 
	 def withofferstartdate____7(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____7 = value 
		 return this 
 	 } 
	 def withofferexpirydate____7(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____7 = value 
		 return this 
 	 } 
	 def withoffertype____7(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____7 = value 
		 return this 
 	 } 
	 def withofferidentifier____8(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____8 = value 
		 return this 
 	 } 
	 def withofferstartdate____8(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____8 = value 
		 return this 
 	 } 
	 def withofferexpirydate____8(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____8 = value 
		 return this 
 	 } 
	 def withoffertype____8(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____8 = value 
		 return this 
 	 } 
	 def withofferidentifier____9(value: Int) : CS5_AIR_REFILL_MA = {
		 this.offeridentifier____9 = value 
		 return this 
 	 } 
	 def withofferstartdate____9(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerstartdate____9 = value 
		 return this 
 	 } 
	 def withofferexpirydate____9(value: String) : CS5_AIR_REFILL_MA = {
		 this.offerexpirydate____9 = value 
		 return this 
 	 } 
	 def withoffertype____9(value: String) : CS5_AIR_REFILL_MA = {
		 this.offertype____9 = value 
		 return this 
 	 } 
	 def withrefillpromodivisionamount(value: String) : CS5_AIR_REFILL_MA = {
		 this.refillpromodivisionamount = value 
		 return this 
 	 } 
	 def withsupervisiondayspromopart(value: Int) : CS5_AIR_REFILL_MA = {
		 this.supervisiondayspromopart = value 
		 return this 
 	 } 
	 def withsupervisiondayssurplus(value: Int) : CS5_AIR_REFILL_MA = {
		 this.supervisiondayssurplus = value 
		 return this 
 	 } 
	 def withservicefeedayspromopart(value: Int) : CS5_AIR_REFILL_MA = {
		 this.servicefeedayspromopart = value 
		 return this 
 	 } 
	 def withservicefeedayssurplus(value: Int) : CS5_AIR_REFILL_MA = {
		 this.servicefeedayssurplus = value 
		 return this 
 	 } 
	 def withmaximumservicefeeperiod(value: Int) : CS5_AIR_REFILL_MA = {
		 this.maximumservicefeeperiod = value 
		 return this 
 	 } 
	 def withmaximumsupervisionperiod(value: Int) : CS5_AIR_REFILL_MA = {
		 this.maximumsupervisionperiod = value 
		 return this 
 	 } 
	 def withactivationdate(value: String) : CS5_AIR_REFILL_MA = {
		 this.activationdate = value 
		 return this 
 	 } 
	 def withwelcomestatus(value: String) : CS5_AIR_REFILL_MA = {
		 this.welcomestatus = value 
		 return this 
 	 } 
	 def withvoucheragent(value: String) : CS5_AIR_REFILL_MA = {
		 this.voucheragent = value 
		 return this 
 	 } 
	 def withpromotionplanallocstartdate(value: String) : CS5_AIR_REFILL_MA = {
		 this.promotionplanallocstartdate = value 
		 return this 
 	 } 
	 def withaccountgroupid(value: Int) : CS5_AIR_REFILL_MA = {
		 this.accountgroupid = value 
		 return this 
 	 } 
	 def withexternaldata1(value: String) : CS5_AIR_REFILL_MA = {
		 this.externaldata1 = value 
		 return this 
 	 } 
	 def withexternaldata2(value: String) : CS5_AIR_REFILL_MA = {
		 this.externaldata2 = value 
		 return this 
 	 } 
	 def withexternaldata3(value: String) : CS5_AIR_REFILL_MA = {
		 this.externaldata3 = value 
		 return this 
 	 } 
	 def withexternaldata4(value: String) : CS5_AIR_REFILL_MA = {
		 this.externaldata4 = value 
		 return this 
 	 } 
	 def withlocationnumber(value: String) : CS5_AIR_REFILL_MA = {
		 this.locationnumber = value 
		 return this 
 	 } 
	 def withvoucheractivationcode(value: String) : CS5_AIR_REFILL_MA = {
		 this.voucheractivationcode = value 
		 return this 
 	 } 
	 def withaccountcurrencycleared(value: String) : CS5_AIR_REFILL_MA = {
		 this.accountcurrencycleared = value 
		 return this 
 	 } 
	 def withignoreserviceclasshierarchy(value: String) : CS5_AIR_REFILL_MA = {
		 this.ignoreserviceclasshierarchy = value 
		 return this 
 	 } 
	 def withselectiontreeid(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeid = value 
		 return this 
 	 } 
	 def withselectiontreeversion(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeversion = value 
		 return this 
 	 } 
	 def withselectiontreeid____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeid____1 = value 
		 return this 
 	 } 
	 def withselectiontreeversion____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeversion____1 = value 
		 return this 
 	 } 
	 def withselectiontreeid____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeid____2 = value 
		 return this 
 	 } 
	 def withselectiontreeversion____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeversion____2 = value 
		 return this 
 	 } 
	 def withselectiontreeid____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeid____3 = value 
		 return this 
 	 } 
	 def withselectiontreeversion____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeversion____3 = value 
		 return this 
 	 } 
	 def withselectiontreeid____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeid____4 = value 
		 return this 
 	 } 
	 def withselectiontreeversion____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.selectiontreeversion____4 = value 
		 return this 
 	 } 
	 def withparameterid(value: String) : CS5_AIR_REFILL_MA = {
		 this.parameterid = value 
		 return this 
 	 } 
	 def withparametervalue(value: String) : CS5_AIR_REFILL_MA = {
		 this.parametervalue = value 
		 return this 
 	 } 
	 def withparameterid____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.parameterid____1 = value 
		 return this 
 	 } 
	 def withparametervalue____1(value: String) : CS5_AIR_REFILL_MA = {
		 this.parametervalue____1 = value 
		 return this 
 	 } 
	 def withparameterid____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.parameterid____2 = value 
		 return this 
 	 } 
	 def withparametervalue____2(value: String) : CS5_AIR_REFILL_MA = {
		 this.parametervalue____2 = value 
		 return this 
 	 } 
	 def withparameterid____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.parameterid____3 = value 
		 return this 
 	 } 
	 def withparametervalue____3(value: String) : CS5_AIR_REFILL_MA = {
		 this.parametervalue____3 = value 
		 return this 
 	 } 
	 def withparameterid____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.parameterid____4 = value 
		 return this 
 	 } 
	 def withparametervalue____4(value: String) : CS5_AIR_REFILL_MA = {
		 this.parametervalue____4 = value 
		 return this 
 	 } 
	 def withaccounthomeregion(value: Int) : CS5_AIR_REFILL_MA = {
		 this.accounthomeregion = value 
		 return this 
 	 } 
	 def withsubscriberregion(value: Int) : CS5_AIR_REFILL_MA = {
		 this.subscriberregion = value 
		 return this 
 	 } 
	 def withvoucherregion(value: Int) : CS5_AIR_REFILL_MA = {
		 this.voucherregion = value 
		 return this 
 	 } 
	 def withpromotionplanallocenddate(value: String) : CS5_AIR_REFILL_MA = {
		 this.promotionplanallocenddate = value 
		 return this 
 	 } 
	 def withrequestedrefilltype(value: Int) : CS5_AIR_REFILL_MA = {
		 this.requestedrefilltype = value 
		 return this 
 	 } 
	 def withcellglobalid(value: String) : CS5_AIR_REFILL_MA = {
		 this.cellglobalid = value 
		 return this 
 	 } 
	 def withdasizeafter(value: String) : CS5_AIR_REFILL_MA = {
		 this.dasizeafter = value 
		 return this 
 	 } 
	 def withdasizebefore(value: String) : CS5_AIR_REFILL_MA = {
		 this.dasizebefore = value 
		 return this 
 	 } 
	 def withaccsizebefore(value: String) : CS5_AIR_REFILL_MA = {
		 this.accsizebefore = value 
		 return this 
 	 } 
	 def withaccsizeafter(value: String) : CS5_AIR_REFILL_MA = {
		 this.accsizeafter = value 
		 return this 
 	 } 
	 def withserviceclassid(value: String) : CS5_AIR_REFILL_MA = {
		 this.serviceclassid = value 
		 return this 
 	 } 
	 def withpaytype(value: String) : CS5_AIR_REFILL_MA = {
		 this.paytype = value 
		 return this 
 	 } 
	 def withcallreferenceid(value: String) : CS5_AIR_REFILL_MA = {
		 this.callreferenceid = value 
		 return this 
 	 } 
	 def withmainamountused(value: String) : CS5_AIR_REFILL_MA = {
		 this.mainamountused = value 
		 return this 
 	 } 
	 def withfile_name(value: String) : CS5_AIR_REFILL_MA = {
		 this.file_name = value 
		 return this 
 	 } 
	 def withfile_offset(value: Long) : CS5_AIR_REFILL_MA = {
		 this.file_offset = value 
		 return this 
 	 } 
	 def withkamanja_loaded_date(value: String) : CS5_AIR_REFILL_MA = {
		 this.kamanja_loaded_date = value 
		 return this 
 	 } 
	 def withfile_mod_date(value: String) : CS5_AIR_REFILL_MA = {
		 this.file_mod_date = value 
		 return this 
 	 } 
	 def withrecharge_type_enrich(value: Int) : CS5_AIR_REFILL_MA = {
		 this.recharge_type_enrich = value 
		 return this 
 	 } 
	 def withdate_key(value: Int) : CS5_AIR_REFILL_MA = {
		 this.date_key = value 
		 return this 
 	 } 
	 def withmsisdn_key(value: Long) : CS5_AIR_REFILL_MA = {
		 this.msisdn_key = value 
		 return this 
 	 } 
	 def withevent_timestamp_enrich(value: Long) : CS5_AIR_REFILL_MA = {
		 this.event_timestamp_enrich = value 
		 return this 
 	 } 
	 def withoriginal_timestamp_enrich(value: String) : CS5_AIR_REFILL_MA = {
		 this.original_timestamp_enrich = value 
		 return this 
 	 } 
	 def withtransaction_amt(value: Double) : CS5_AIR_REFILL_MA = {
		 this.transaction_amt = value 
		 return this 
 	 } 
	 def withorigin_time_stamp(value: String) : CS5_AIR_REFILL_MA = {
		 this.origin_time_stamp = value 
		 return this 
 	 } 
	 def withvoucher_serial_nr(value: String) : CS5_AIR_REFILL_MA = {
		 this.voucher_serial_nr = value 
		 return this 
 	 } 
	 def withorigin_host_nm(value: String) : CS5_AIR_REFILL_MA = {
		 this.origin_host_nm = value 
		 return this 
 	 } 
	 def withexternal_data1(value: String) : CS5_AIR_REFILL_MA = {
		 this.external_data1 = value 
		 return this 
 	 } 
	 def withmsg_unique_id_enrich(value: Long) : CS5_AIR_REFILL_MA = {
		 this.msg_unique_id_enrich = value 
		 return this 
 	 } 
	 def withbase_file_name(value: String) : CS5_AIR_REFILL_MA = {
		 this.base_file_name = value 
		 return this 
 	 } 
	 def withpath(value: String) : CS5_AIR_REFILL_MA = {
		 this.path = value 
		 return this 
 	 } 
	 def withline_number(value: Long) : CS5_AIR_REFILL_MA = {
		 this.line_number = value 
		 return this 
 	 } 
	 def withfile_id(value: String) : CS5_AIR_REFILL_MA = {
		 this.file_id = value 
		 return this 
 	 } 
	 def withprocessed_timestamp(value: Long) : CS5_AIR_REFILL_MA = {
		 this.processed_timestamp = value 
		 return this 
 	 } 
	 def withltz_event_timestamp_enrich(value: Long) : CS5_AIR_REFILL_MA = {
		 this.ltz_event_timestamp_enrich = value 
		 return this 
 	 } 
	 def withkamanja_system_null_flags(value: scala.Array[Boolean]) : CS5_AIR_REFILL_MA = {
		 this.kamanja_system_null_flags = value 
		 return this 
 	 } 



    override def getRDDObject(): RDDObject[ContainerInterface] = CS5_AIR_REFILL_MA.asInstanceOf[RDDObject[ContainerInterface]];


        def isCaseSensitive(): Boolean = CS5_AIR_REFILL_MA.isCaseSensitive(); 
    def caseSensitiveKey(keyName: String): String = {
      if(isCaseSensitive)
        return keyName;
      else return keyName.toLowerCase;
    }


    
    def this(factory:MessageFactoryInterface) = {
      this(factory, null)
     }
    
    def this(other: CS5_AIR_REFILL_MA) = {
      this(other.getFactory.asInstanceOf[MessageFactoryInterface], other)
    }

}