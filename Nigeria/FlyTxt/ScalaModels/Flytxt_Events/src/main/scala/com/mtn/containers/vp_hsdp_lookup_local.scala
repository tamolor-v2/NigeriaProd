package com.mtn.containers;
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
    
 
object vp_hsdp_lookup extends RDDObject[vp_hsdp_lookup] with ContainerFactoryInterface { 
 
  val log = LogManager.getLogger(getClass)
	type T = vp_hsdp_lookup ;
	override def getFullTypeName: String = "com.mtn.containers.vp_hsdp_lookup"; 
	override def getTypeNameSpace: String = "com.mtn.containers"; 
	override def getTypeName: String = "vp_hsdp_lookup"; 
	override def getTypeVersion: String = "000000.000001.000000"; 
	override def getSchemaId: Int = 2000554; 
	private var elementId: Long = 0L; 
	override def setElementId(elemId: Long): Unit = { elementId = elemId; } 
	override def getElementId: Long = elementId; 
	override def getTenantId: String = "flare"; 
	override def createInstance: vp_hsdp_lookup = new vp_hsdp_lookup(vp_hsdp_lookup); 
	override def isFixed: Boolean = true; 
	def isCaseSensitive(): Boolean = false; 
	override def getContainerType: ContainerTypes.ContainerType = ContainerTypes.ContainerType.CONTAINER
	override def getFullName = getFullTypeName; 
	override def getRddTenantId = getTenantId; 
	override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this); 

    def build = new T(this)
    def build(from: T) = new T(from)
   override def getPartitionKeyNames: Array[String] = Array("product_id"); 

  override def getPrimaryKeyNames: Array[String] = Array("product_id"); 
   
  
  override def getTimePartitionInfo: TimePartitionInfo = { return null;}  // FieldName, Format & Time Partition Types(Daily/Monthly/Yearly)
  
       
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
  
    override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.containers" , "name" : "vp_hsdp_lookup" , "fields":[{ "name" : "opco_name" , "type" : "string"},{ "name" : "partner_name" , "type" : "string"},{ "name" : "partner_id" , "type" : "string"},{ "name" : "service_name" , "type" : "string"},{ "name" : "service_id" , "type" : "string"},{ "name" : "product_name" , "type" : "string"},{ "name" : "product_id" , "type" : "string"},{ "name" : "status" , "type" : "string"},{ "name" : "product_type" , "type" : "string"},{ "name" : "sms_access_code" , "type" : "string"},{ "name" : "ussd_access_code" , "type" : "string"},{ "name" : "opt_in_keyword" , "type" : "string"},{ "name" : "subsciption_fee" , "type" : "string"},{ "name" : "opt_out_keyword" , "type" : "string"},{ "name" : "on_demand_fee" , "type" : "string"},{ "name" : "product_online_date" , "type" : "string"},{ "name" : "total_subscriptions" , "type" : "string"},{ "name" : "on_demand_request" , "type" : "string"},{ "name" : "service_category" , "type" : "string"},{ "name" : "service_sub_category" , "type" : "string"},{ "name" : "product_category" , "type" : "string"},{ "name" : "finance_category" , "type" : "string"},{ "name" : "mkt_category" , "type" : "string"},{ "name" : "platform_revenue" , "type" : "string"},{ "name" : "mod_catogory" , "type" : "string"},{ "name" : "is_mod" , "type" : "string"}]}""";  
  
      var attributeTypes = generateAttributeTypes;
    
    private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
      var attributeTypes = new Array[AttributeTypeInfo](26);
                                                                        		 attributeTypes(0) = new AttributeTypeInfo("opco_name", 0, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(1) = new AttributeTypeInfo("partner_name", 1, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(2) = new AttributeTypeInfo("partner_id", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(3) = new AttributeTypeInfo("service_name", 3, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(4) = new AttributeTypeInfo("service_id", 4, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(5) = new AttributeTypeInfo("product_name", 5, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(6) = new AttributeTypeInfo("product_id", 6, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(7) = new AttributeTypeInfo("status", 7, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(8) = new AttributeTypeInfo("product_type", 8, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(9) = new AttributeTypeInfo("sms_access_code", 9, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(10) = new AttributeTypeInfo("ussd_access_code", 10, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(11) = new AttributeTypeInfo("opt_in_keyword", 11, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(12) = new AttributeTypeInfo("subsciption_fee", 12, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(13) = new AttributeTypeInfo("opt_out_keyword", 13, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(14) = new AttributeTypeInfo("on_demand_fee", 14, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(15) = new AttributeTypeInfo("product_online_date", 15, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(16) = new AttributeTypeInfo("total_subscriptions", 16, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(17) = new AttributeTypeInfo("on_demand_request", 17, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(18) = new AttributeTypeInfo("service_category", 18, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(19) = new AttributeTypeInfo("service_sub_category", 19, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(20) = new AttributeTypeInfo("product_category", 20, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(21) = new AttributeTypeInfo("finance_category", 21, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(22) = new AttributeTypeInfo("mkt_category", 22, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(23) = new AttributeTypeInfo("platform_revenue", 23, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(24) = new AttributeTypeInfo("mod_catogory", 24, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(25) = new AttributeTypeInfo("is_mod", 25, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)


      return attributeTypes
    }
    

		 var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
				def setFn0(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.opco_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field opco_name in message vp_hsdp_lookup") 
				} 
				def setFn1(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.partner_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field partner_name in message vp_hsdp_lookup") 
				} 
				def setFn2(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.partner_id = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field partner_id in message vp_hsdp_lookup") 
				} 
				def setFn3(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.service_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field service_name in message vp_hsdp_lookup") 
				} 
				def setFn4(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.service_id = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field service_id in message vp_hsdp_lookup") 
				} 
				def setFn5(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.product_name = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field product_name in message vp_hsdp_lookup") 
				} 
				def setFn6(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.product_id = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field product_id in message vp_hsdp_lookup") 
				} 
				def setFn7(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.status = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field status in message vp_hsdp_lookup") 
				} 
				def setFn8(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.product_type = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field product_type in message vp_hsdp_lookup") 
				} 
				def setFn9(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.sms_access_code = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field sms_access_code in message vp_hsdp_lookup") 
				} 
				def setFn10(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.ussd_access_code = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field ussd_access_code in message vp_hsdp_lookup") 
				} 
				def setFn11(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.opt_in_keyword = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field opt_in_keyword in message vp_hsdp_lookup") 
				} 
				def setFn12(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.subsciption_fee = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field subsciption_fee in message vp_hsdp_lookup") 
				} 
				def setFn13(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.opt_out_keyword = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field opt_out_keyword in message vp_hsdp_lookup") 
				} 
				def setFn14(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.on_demand_fee = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field on_demand_fee in message vp_hsdp_lookup") 
				} 
				def setFn15(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.product_online_date = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field product_online_date in message vp_hsdp_lookup") 
				} 
				def setFn16(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.total_subscriptions = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field total_subscriptions in message vp_hsdp_lookup") 
				} 
				def setFn17(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.on_demand_request = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field on_demand_request in message vp_hsdp_lookup") 
				} 
				def setFn18(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.service_category = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field service_category in message vp_hsdp_lookup") 
				} 
				def setFn19(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.service_sub_category = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field service_sub_category in message vp_hsdp_lookup") 
				} 
				def setFn20(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.product_category = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field product_category in message vp_hsdp_lookup") 
				} 
				def setFn21(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.finance_category = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field finance_category in message vp_hsdp_lookup") 
				} 
				def setFn22(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.mkt_category = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field mkt_category in message vp_hsdp_lookup") 
				} 
				def setFn23(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.platform_revenue = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field platform_revenue in message vp_hsdp_lookup") 
				} 
				def setFn24(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.mod_catogory = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field mod_catogory in message vp_hsdp_lookup") 
				} 
				def setFn25(curObj: vp_hsdp_lookup, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.is_mod = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field is_mod in message vp_hsdp_lookup") 
				} 

    val setFnArr = Array[(vp_hsdp_lookup, Any) => Unit](setFn0,setFn1,setFn2,setFn3,setFn4,setFn5,setFn6,setFn7,setFn8,setFn9,setFn10,setFn11,setFn12,setFn13,setFn14,setFn15,setFn16,setFn17,setFn18,setFn19,setFn20,setFn21,setFn22,setFn23,setFn24,setFn25    )
		def getFn0(curObj: vp_hsdp_lookup): AnyRef = curObj.opco_name.asInstanceOf[AnyRef]; 
		def getFn1(curObj: vp_hsdp_lookup): AnyRef = curObj.partner_name.asInstanceOf[AnyRef]; 
		def getFn2(curObj: vp_hsdp_lookup): AnyRef = curObj.partner_id.asInstanceOf[AnyRef]; 
		def getFn3(curObj: vp_hsdp_lookup): AnyRef = curObj.service_name.asInstanceOf[AnyRef]; 
		def getFn4(curObj: vp_hsdp_lookup): AnyRef = curObj.service_id.asInstanceOf[AnyRef]; 
		def getFn5(curObj: vp_hsdp_lookup): AnyRef = curObj.product_name.asInstanceOf[AnyRef]; 
		def getFn6(curObj: vp_hsdp_lookup): AnyRef = curObj.product_id.asInstanceOf[AnyRef]; 
		def getFn7(curObj: vp_hsdp_lookup): AnyRef = curObj.status.asInstanceOf[AnyRef]; 
		def getFn8(curObj: vp_hsdp_lookup): AnyRef = curObj.product_type.asInstanceOf[AnyRef]; 
		def getFn9(curObj: vp_hsdp_lookup): AnyRef = curObj.sms_access_code.asInstanceOf[AnyRef]; 
		def getFn10(curObj: vp_hsdp_lookup): AnyRef = curObj.ussd_access_code.asInstanceOf[AnyRef]; 
		def getFn11(curObj: vp_hsdp_lookup): AnyRef = curObj.opt_in_keyword.asInstanceOf[AnyRef]; 
		def getFn12(curObj: vp_hsdp_lookup): AnyRef = curObj.subsciption_fee.asInstanceOf[AnyRef]; 
		def getFn13(curObj: vp_hsdp_lookup): AnyRef = curObj.opt_out_keyword.asInstanceOf[AnyRef]; 
		def getFn14(curObj: vp_hsdp_lookup): AnyRef = curObj.on_demand_fee.asInstanceOf[AnyRef]; 
		def getFn15(curObj: vp_hsdp_lookup): AnyRef = curObj.product_online_date.asInstanceOf[AnyRef]; 
		def getFn16(curObj: vp_hsdp_lookup): AnyRef = curObj.total_subscriptions.asInstanceOf[AnyRef]; 
		def getFn17(curObj: vp_hsdp_lookup): AnyRef = curObj.on_demand_request.asInstanceOf[AnyRef]; 
		def getFn18(curObj: vp_hsdp_lookup): AnyRef = curObj.service_category.asInstanceOf[AnyRef]; 
		def getFn19(curObj: vp_hsdp_lookup): AnyRef = curObj.service_sub_category.asInstanceOf[AnyRef]; 
		def getFn20(curObj: vp_hsdp_lookup): AnyRef = curObj.product_category.asInstanceOf[AnyRef]; 
		def getFn21(curObj: vp_hsdp_lookup): AnyRef = curObj.finance_category.asInstanceOf[AnyRef]; 
		def getFn22(curObj: vp_hsdp_lookup): AnyRef = curObj.mkt_category.asInstanceOf[AnyRef]; 
		def getFn23(curObj: vp_hsdp_lookup): AnyRef = curObj.platform_revenue.asInstanceOf[AnyRef]; 
		def getFn24(curObj: vp_hsdp_lookup): AnyRef = curObj.mod_catogory.asInstanceOf[AnyRef]; 
		def getFn25(curObj: vp_hsdp_lookup): AnyRef = curObj.is_mod.asInstanceOf[AnyRef]; 

    val getFnArr = Array[(vp_hsdp_lookup) => AnyRef](getFn0,getFn1,getFn2,getFn3,getFn4,getFn5,getFn6,getFn7,getFn8,getFn9,getFn10,getFn11,getFn12,getFn13,getFn14,getFn15,getFn16,getFn17,getFn18,getFn19,getFn20,getFn21,getFn22,getFn23,getFn24,getFn25    )



    override def getRDDObject(): RDDObject[ContainerInterface] = vp_hsdp_lookup.asInstanceOf[RDDObject[ContainerInterface]];


    
    final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);
      
    override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
      try {
        if (oldVerobj == null) return null;
        oldVerobj match {
          
      case oldVerobj: com.mtn.containers.vp_hsdp_lookup => { return  convertToVer1000000(oldVerobj); } 
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
  
    private def convertToVer1000000(oldVerobj: com.mtn.containers.vp_hsdp_lookup): com.mtn.containers.vp_hsdp_lookup= {
      return oldVerobj
    }
  
      
  /****   DEPRECATED METHODS ***/
  override def FullName: String = getFullTypeName
  override def NameSpace: String = getTypeNameSpace
  override def Name: String = getTypeName
  override def Version: String = getTypeVersion
  override def CreateNewMessage: BaseMsg= null;
  override def CreateNewContainer: BaseContainer= createInstance.asInstanceOf[BaseContainer];
  override def IsFixed: Boolean = true
  override def IsKv: Boolean = false
  override def CanPersist: Boolean = true
  override def isMessage: Boolean = false
  override def isContainer: Boolean = true
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj vp_hsdp_lookup") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj vp_hsdp_lookup");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj vp_hsdp_lookup");
 override def NeedToTransformData: Boolean = false
    }

class vp_hsdp_lookup(factory: ContainerFactoryInterface, other: vp_hsdp_lookup) extends ContainerInterface(factory) { 
 
  val log = vp_hsdp_lookup.log

      var attributeTypes = vp_hsdp_lookup.attributeTypes
      
		 var keyTypes = vp_hsdp_lookup.keyTypes;
    
     if (other != null && other != this) {
      // call copying fields from other to local variables
      fromFunc(other)
    }
    
    override def save: Unit = { vp_hsdp_lookup.saveOne(this) }
  
    def Clone(): ContainerOrConcept = { vp_hsdp_lookup.build(this) }

		override def getPartitionKey: Array[String] = {
		var partitionKeys: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
		try {
		 partitionKeys += com.ligadata.BaseTypes.StringImpl.toString(get(caseSensitiveKey("product_id")).asInstanceOf[String]);
		 }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      		 partitionKeys.toArray; 

 		} 
 

		override def getPrimaryKey: Array[String] = {
		var primaryKeys: scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]();
		try {
		 primaryKeys += com.ligadata.BaseTypes.StringImpl.toString(get(caseSensitiveKey("product_id")).asInstanceOf[String]);
		 }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      		 primaryKeys.toArray; 

 		} 
 

    override def getAttributeType(name: String): AttributeTypeInfo = {
      if (name == null || name.trim() == "") return null;
      keyTypes.getOrElse(caseSensitiveKey(name), null)
    }
  
  
      var setFnArr = vp_hsdp_lookup.setFnArr
    
      var getFnArr = vp_hsdp_lookup.getFnArr
    
 		var opco_name : String = _; 
 		var partner_name : String = _; 
 		var partner_id : String = _; 
 		var service_name : String = _; 
 		var service_id : String = _; 
 		var product_name : String = _; 
 		var product_id : String = _; 
 		var status : String = _; 
 		var product_type : String = _; 
 		var sms_access_code : String = _; 
 		var ussd_access_code : String = _; 
 		var opt_in_keyword : String = _; 
 		var subsciption_fee : String = _; 
 		var opt_out_keyword : String = _; 
 		var on_demand_fee : String = _; 
 		var product_online_date : String = _; 
 		var total_subscriptions : String = _; 
 		var on_demand_request : String = _; 
 		var service_category : String = _; 
 		var service_sub_category : String = _; 
 		var product_category : String = _; 
 		var finance_category : String = _; 
 		var mkt_category : String = _; 
 		var platform_revenue : String = _; 
 		var mod_catogory : String = _; 
 		var is_mod : String = _; 

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
      val fieldX = ru.typeOf[vp_hsdp_lookup].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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
   
      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container vp_hsdp_lookup", null);
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
      throw new Exception(s"$index is out of range for message vp_hsdp_lookup");
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
      var attributeVals = new Array[AttributeValue](26);
      try{
 				attributeVals(0) = new AttributeValue(this.opco_name, attributeTypes(0)) 
				attributeVals(1) = new AttributeValue(this.partner_name, attributeTypes(1)) 
				attributeVals(2) = new AttributeValue(this.partner_id, attributeTypes(2)) 
				attributeVals(3) = new AttributeValue(this.service_name, attributeTypes(3)) 
				attributeVals(4) = new AttributeValue(this.service_id, attributeTypes(4)) 
				attributeVals(5) = new AttributeValue(this.product_name, attributeTypes(5)) 
				attributeVals(6) = new AttributeValue(this.product_id, attributeTypes(6)) 
				attributeVals(7) = new AttributeValue(this.status, attributeTypes(7)) 
				attributeVals(8) = new AttributeValue(this.product_type, attributeTypes(8)) 
				attributeVals(9) = new AttributeValue(this.sms_access_code, attributeTypes(9)) 
				attributeVals(10) = new AttributeValue(this.ussd_access_code, attributeTypes(10)) 
				attributeVals(11) = new AttributeValue(this.opt_in_keyword, attributeTypes(11)) 
				attributeVals(12) = new AttributeValue(this.subsciption_fee, attributeTypes(12)) 
				attributeVals(13) = new AttributeValue(this.opt_out_keyword, attributeTypes(13)) 
				attributeVals(14) = new AttributeValue(this.on_demand_fee, attributeTypes(14)) 
				attributeVals(15) = new AttributeValue(this.product_online_date, attributeTypes(15)) 
				attributeVals(16) = new AttributeValue(this.total_subscriptions, attributeTypes(16)) 
				attributeVals(17) = new AttributeValue(this.on_demand_request, attributeTypes(17)) 
				attributeVals(18) = new AttributeValue(this.service_category, attributeTypes(18)) 
				attributeVals(19) = new AttributeValue(this.service_sub_category, attributeTypes(19)) 
				attributeVals(20) = new AttributeValue(this.product_category, attributeTypes(20)) 
				attributeVals(21) = new AttributeValue(this.finance_category, attributeTypes(21)) 
				attributeVals(22) = new AttributeValue(this.mkt_category, attributeTypes(22)) 
				attributeVals(23) = new AttributeValue(this.platform_revenue, attributeTypes(23)) 
				attributeVals(24) = new AttributeValue(this.mod_catogory, attributeTypes(24)) 
				attributeVals(25) = new AttributeValue(this.is_mod, attributeTypes(25)) 
       
      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return attributeVals;
    }      
    
    override def getOnlyValuesForAllAttributes(): Array[Object] = {
      var allVals = new Array[Object](26);
      try{
 				allVals(0) = this.opco_name.asInstanceOf[AnyRef]; 
				allVals(1) = this.partner_name.asInstanceOf[AnyRef]; 
				allVals(2) = this.partner_id.asInstanceOf[AnyRef]; 
				allVals(3) = this.service_name.asInstanceOf[AnyRef]; 
				allVals(4) = this.service_id.asInstanceOf[AnyRef]; 
				allVals(5) = this.product_name.asInstanceOf[AnyRef]; 
				allVals(6) = this.product_id.asInstanceOf[AnyRef]; 
				allVals(7) = this.status.asInstanceOf[AnyRef]; 
				allVals(8) = this.product_type.asInstanceOf[AnyRef]; 
				allVals(9) = this.sms_access_code.asInstanceOf[AnyRef]; 
				allVals(10) = this.ussd_access_code.asInstanceOf[AnyRef]; 
				allVals(11) = this.opt_in_keyword.asInstanceOf[AnyRef]; 
				allVals(12) = this.subsciption_fee.asInstanceOf[AnyRef]; 
				allVals(13) = this.opt_out_keyword.asInstanceOf[AnyRef]; 
				allVals(14) = this.on_demand_fee.asInstanceOf[AnyRef]; 
				allVals(15) = this.product_online_date.asInstanceOf[AnyRef]; 
				allVals(16) = this.total_subscriptions.asInstanceOf[AnyRef]; 
				allVals(17) = this.on_demand_request.asInstanceOf[AnyRef]; 
				allVals(18) = this.service_category.asInstanceOf[AnyRef]; 
				allVals(19) = this.service_sub_category.asInstanceOf[AnyRef]; 
				allVals(20) = this.product_category.asInstanceOf[AnyRef]; 
				allVals(21) = this.finance_category.asInstanceOf[AnyRef]; 
				allVals(22) = this.mkt_category.asInstanceOf[AnyRef]; 
				allVals(23) = this.platform_revenue.asInstanceOf[AnyRef]; 
				allVals(24) = this.mod_catogory.asInstanceOf[AnyRef]; 
				allVals(25) = this.is_mod.asInstanceOf[AnyRef]; 

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
   
  			 if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message vp_hsdp_lookup", null)
			 set(keyTypes(key).getIndex, value); 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
    }
  
      
    def set(index : Int, value :Any): Unit = {
      if (value == null) throw new Exception(s"Value is null for index $index in message vp_hsdp_lookup ")
      if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message vp_hsdp_lookup ")
      setFnArr(index)(this, value)
    }
    
    override def set(key: String, value: Any, valTyp: String) = {
      throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
    }
  
    private def fromFunc(other: vp_hsdp_lookup): vp_hsdp_lookup = {  
   			this.opco_name = com.ligadata.BaseTypes.StringImpl.Clone(other.opco_name);
			this.partner_name = com.ligadata.BaseTypes.StringImpl.Clone(other.partner_name);
			this.partner_id = com.ligadata.BaseTypes.StringImpl.Clone(other.partner_id);
			this.service_name = com.ligadata.BaseTypes.StringImpl.Clone(other.service_name);
			this.service_id = com.ligadata.BaseTypes.StringImpl.Clone(other.service_id);
			this.product_name = com.ligadata.BaseTypes.StringImpl.Clone(other.product_name);
			this.product_id = com.ligadata.BaseTypes.StringImpl.Clone(other.product_id);
			this.status = com.ligadata.BaseTypes.StringImpl.Clone(other.status);
			this.product_type = com.ligadata.BaseTypes.StringImpl.Clone(other.product_type);
			this.sms_access_code = com.ligadata.BaseTypes.StringImpl.Clone(other.sms_access_code);
			this.ussd_access_code = com.ligadata.BaseTypes.StringImpl.Clone(other.ussd_access_code);
			this.opt_in_keyword = com.ligadata.BaseTypes.StringImpl.Clone(other.opt_in_keyword);
			this.subsciption_fee = com.ligadata.BaseTypes.StringImpl.Clone(other.subsciption_fee);
			this.opt_out_keyword = com.ligadata.BaseTypes.StringImpl.Clone(other.opt_out_keyword);
			this.on_demand_fee = com.ligadata.BaseTypes.StringImpl.Clone(other.on_demand_fee);
			this.product_online_date = com.ligadata.BaseTypes.StringImpl.Clone(other.product_online_date);
			this.total_subscriptions = com.ligadata.BaseTypes.StringImpl.Clone(other.total_subscriptions);
			this.on_demand_request = com.ligadata.BaseTypes.StringImpl.Clone(other.on_demand_request);
			this.service_category = com.ligadata.BaseTypes.StringImpl.Clone(other.service_category);
			this.service_sub_category = com.ligadata.BaseTypes.StringImpl.Clone(other.service_sub_category);
			this.product_category = com.ligadata.BaseTypes.StringImpl.Clone(other.product_category);
			this.finance_category = com.ligadata.BaseTypes.StringImpl.Clone(other.finance_category);
			this.mkt_category = com.ligadata.BaseTypes.StringImpl.Clone(other.mkt_category);
			this.platform_revenue = com.ligadata.BaseTypes.StringImpl.Clone(other.platform_revenue);
			this.mod_catogory = com.ligadata.BaseTypes.StringImpl.Clone(other.mod_catogory);
			this.is_mod = com.ligadata.BaseTypes.StringImpl.Clone(other.is_mod);

      this.setTimePartitionData(com.ligadata.BaseTypes.LongImpl.Clone(other.getTimePartitionData));
      return this;
    }
    

    override def computePartitionKeyHashIdValue(): Long = {
    var initialseed: Long = 0xcbf29ce484222325L
    var curVal: Long = initialseed
    
    val partitionKeys = Array[Any](product_id)                
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
 	 def withopco_name(value: String) : vp_hsdp_lookup = {
		 this.opco_name = value 
		 return this 
 	 } 
	 def withpartner_name(value: String) : vp_hsdp_lookup = {
		 this.partner_name = value 
		 return this 
 	 } 
	 def withpartner_id(value: String) : vp_hsdp_lookup = {
		 this.partner_id = value 
		 return this 
 	 } 
	 def withservice_name(value: String) : vp_hsdp_lookup = {
		 this.service_name = value 
		 return this 
 	 } 
	 def withservice_id(value: String) : vp_hsdp_lookup = {
		 this.service_id = value 
		 return this 
 	 } 
	 def withproduct_name(value: String) : vp_hsdp_lookup = {
		 this.product_name = value 
		 return this 
 	 } 
	 def withproduct_id(value: String) : vp_hsdp_lookup = {
		 this.product_id = value 
		 return this 
 	 } 
	 def withstatus(value: String) : vp_hsdp_lookup = {
		 this.status = value 
		 return this 
 	 } 
	 def withproduct_type(value: String) : vp_hsdp_lookup = {
		 this.product_type = value 
		 return this 
 	 } 
	 def withsms_access_code(value: String) : vp_hsdp_lookup = {
		 this.sms_access_code = value 
		 return this 
 	 } 
	 def withussd_access_code(value: String) : vp_hsdp_lookup = {
		 this.ussd_access_code = value 
		 return this 
 	 } 
	 def withopt_in_keyword(value: String) : vp_hsdp_lookup = {
		 this.opt_in_keyword = value 
		 return this 
 	 } 
	 def withsubsciption_fee(value: String) : vp_hsdp_lookup = {
		 this.subsciption_fee = value 
		 return this 
 	 } 
	 def withopt_out_keyword(value: String) : vp_hsdp_lookup = {
		 this.opt_out_keyword = value 
		 return this 
 	 } 
	 def withon_demand_fee(value: String) : vp_hsdp_lookup = {
		 this.on_demand_fee = value 
		 return this 
 	 } 
	 def withproduct_online_date(value: String) : vp_hsdp_lookup = {
		 this.product_online_date = value 
		 return this 
 	 } 
	 def withtotal_subscriptions(value: String) : vp_hsdp_lookup = {
		 this.total_subscriptions = value 
		 return this 
 	 } 
	 def withon_demand_request(value: String) : vp_hsdp_lookup = {
		 this.on_demand_request = value 
		 return this 
 	 } 
	 def withservice_category(value: String) : vp_hsdp_lookup = {
		 this.service_category = value 
		 return this 
 	 } 
	 def withservice_sub_category(value: String) : vp_hsdp_lookup = {
		 this.service_sub_category = value 
		 return this 
 	 } 
	 def withproduct_category(value: String) : vp_hsdp_lookup = {
		 this.product_category = value 
		 return this 
 	 } 
	 def withfinance_category(value: String) : vp_hsdp_lookup = {
		 this.finance_category = value 
		 return this 
 	 } 
	 def withmkt_category(value: String) : vp_hsdp_lookup = {
		 this.mkt_category = value 
		 return this 
 	 } 
	 def withplatform_revenue(value: String) : vp_hsdp_lookup = {
		 this.platform_revenue = value 
		 return this 
 	 } 
	 def withmod_catogory(value: String) : vp_hsdp_lookup = {
		 this.mod_catogory = value 
		 return this 
 	 } 
	 def withis_mod(value: String) : vp_hsdp_lookup = {
		 this.is_mod = value 
		 return this 
 	 } 



    override def getRDDObject(): RDDObject[ContainerInterface] = vp_hsdp_lookup.asInstanceOf[RDDObject[ContainerInterface]];


        def isCaseSensitive(): Boolean = vp_hsdp_lookup.isCaseSensitive(); 
    def caseSensitiveKey(keyName: String): String = {
      if(isCaseSensitive)
        return keyName;
      else return keyName.toLowerCase;
    }


    
    def this(factory:ContainerFactoryInterface) = {
      this(factory, null)
     }
    
    def this(other: vp_hsdp_lookup) = {
      this(other.getFactory.asInstanceOf[ContainerFactoryInterface], other)
    }

}