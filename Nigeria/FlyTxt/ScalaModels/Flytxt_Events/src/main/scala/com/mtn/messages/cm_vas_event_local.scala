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
    
 
object cm_vas_event extends RDDObject[cm_vas_event] with MessageFactoryInterface { 
 
  val log = LogManager.getLogger(getClass)
	type T = cm_vas_event ;
	override def getFullTypeName: String = "com.mtn.messages.cm_vas_event"; 
	override def getTypeNameSpace: String = "com.mtn.messages"; 
	override def getTypeName: String = "cm_vas_event"; 
	override def getTypeVersion: String = "000000.000001.000000"; 
	override def getSchemaId: Int = 2005531; 
	private var elementId: Long = 0L; 
	override def setElementId(elemId: Long): Unit = { elementId = elemId; } 
	override def getElementId: Long = elementId; 
	override def getTenantId: String = "tenant1"; 
	override def createInstance: cm_vas_event = new cm_vas_event(cm_vas_event); 
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
    timePartitionInfo.setFieldName("tbl_dt");
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
  
    override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.messages" , "name" : "cm_vas_event" , "fields":[{ "name" : "msisdn_key" , "type" : "long"},{ "name" : "tbl_dt" , "type" : "int"},{ "name" : "original_timestamp" , "type" : "string"},{ "name" : "value_of_pack" , "type" : "string"},{ "name" : "name_of_pack" , "type" : "string"},{ "name" : "vas_event_type_identifier" , "type" : "int"},{ "name" : "validity" , "type" : "int"},{ "name" : "main_balance_before_event" , "type" : "string"},{ "name" : "main_balance_after_event" , "type" : "string"},{ "name" : "product_id" , "type" : "string"}]}""";  
  
      var attributeTypes = generateAttributeTypes;
    
    private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
      var attributeTypes = new Array[AttributeTypeInfo](10);
                                                                        		 attributeTypes(0) = new AttributeTypeInfo("msisdn_key", 0, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
		 attributeTypes(1) = new AttributeTypeInfo("tbl_dt", 1, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(2) = new AttributeTypeInfo("original_timestamp", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(3) = new AttributeTypeInfo("value_of_pack", 3, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(4) = new AttributeTypeInfo("name_of_pack", 4, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(5) = new AttributeTypeInfo("vas_event_type_identifier", 5, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(6) = new AttributeTypeInfo("validity", 6, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
		 attributeTypes(7) = new AttributeTypeInfo("main_balance_before_event", 7, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(8) = new AttributeTypeInfo("main_balance_after_event", 8, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
		 attributeTypes(9) = new AttributeTypeInfo("product_id", 9, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)


      return attributeTypes
    }
    

		 var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
				def setFn0(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Long]) 
				  curObj.msisdn_key = value.asInstanceOf[Long]; 
				 else throw new Exception(s"Value is the not the correct type Long for field msisdn_key in message cm_vas_event") 
				} 
				def setFn1(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]){ 
				  curObj.tbl_dt = value.asInstanceOf[Int]; 
				  curObj.setTimePartitionData; 
				} else throw new Exception(s"Value is the not the correct type Int for field tbl_dt in message cm_vas_event") 
				} 
				def setFn2(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.original_timestamp = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field original_timestamp in message cm_vas_event") 
				} 
				def setFn3(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.value_of_pack = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field value_of_pack in message cm_vas_event") 
				} 
				def setFn4(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.name_of_pack = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field name_of_pack in message cm_vas_event") 
				} 
				def setFn5(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.vas_event_type_identifier = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field vas_event_type_identifier in message cm_vas_event") 
				} 
				def setFn6(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[Int]) 
				  curObj.validity = value.asInstanceOf[Int]; 
				 else throw new Exception(s"Value is the not the correct type Int for field validity in message cm_vas_event") 
				} 
				def setFn7(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.main_balance_before_event = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field main_balance_before_event in message cm_vas_event") 
				} 
				def setFn8(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.main_balance_after_event = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field main_balance_after_event in message cm_vas_event") 
				} 
				def setFn9(curObj: cm_vas_event, value: Any): Unit ={ 
				if(value.isInstanceOf[String]) 
				  curObj.product_id = value.asInstanceOf[String]; 
				 else throw new Exception(s"Value is the not the correct type String for field product_id in message cm_vas_event") 
				} 

    val setFnArr = Array[(cm_vas_event, Any) => Unit](setFn0,setFn1,setFn2,setFn3,setFn4,setFn5,setFn6,setFn7,setFn8,setFn9    )
		def getFn0(curObj: cm_vas_event): AnyRef = curObj.msisdn_key.asInstanceOf[AnyRef]; 
		def getFn1(curObj: cm_vas_event): AnyRef = curObj.tbl_dt.asInstanceOf[AnyRef]; 
		def getFn2(curObj: cm_vas_event): AnyRef = curObj.original_timestamp.asInstanceOf[AnyRef]; 
		def getFn3(curObj: cm_vas_event): AnyRef = curObj.value_of_pack.asInstanceOf[AnyRef]; 
		def getFn4(curObj: cm_vas_event): AnyRef = curObj.name_of_pack.asInstanceOf[AnyRef]; 
		def getFn5(curObj: cm_vas_event): AnyRef = curObj.vas_event_type_identifier.asInstanceOf[AnyRef]; 
		def getFn6(curObj: cm_vas_event): AnyRef = curObj.validity.asInstanceOf[AnyRef]; 
		def getFn7(curObj: cm_vas_event): AnyRef = curObj.main_balance_before_event.asInstanceOf[AnyRef]; 
		def getFn8(curObj: cm_vas_event): AnyRef = curObj.main_balance_after_event.asInstanceOf[AnyRef]; 
		def getFn9(curObj: cm_vas_event): AnyRef = curObj.product_id.asInstanceOf[AnyRef]; 

    val getFnArr = Array[(cm_vas_event) => AnyRef](getFn0,getFn1,getFn2,getFn3,getFn4,getFn5,getFn6,getFn7,getFn8,getFn9    )



    override def getRDDObject(): RDDObject[ContainerInterface] = cm_vas_event.asInstanceOf[RDDObject[ContainerInterface]];


    
    final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);
      
    override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
      try {
        if (oldVerobj == null) return null;
        oldVerobj match {
          
      case oldVerobj: com.mtn.messages.cm_vas_event => { return  convertToVer1000000(oldVerobj); } 
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
  
    private def convertToVer1000000(oldVerobj: com.mtn.messages.cm_vas_event): com.mtn.messages.cm_vas_event= {
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
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj cm_vas_event") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj cm_vas_event");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj cm_vas_event");
 override def NeedToTransformData: Boolean = false
    }

class cm_vas_event(factory: MessageFactoryInterface, other: cm_vas_event) extends MessageInterface(factory) { 
 
  val log = cm_vas_event.log

      var attributeTypes = cm_vas_event.attributeTypes
      
		 var keyTypes = cm_vas_event.keyTypes;
    
     if (other != null && other != this) {
      // call copying fields from other to local variables
      fromFunc(other)
    }
    
    override def save: Unit = { cm_vas_event.saveOne(this) }
  
    def Clone(): ContainerOrConcept = { cm_vas_event.build(this) }

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
  
  
      var setFnArr = cm_vas_event.setFnArr
    
      var getFnArr = cm_vas_event.getFnArr
    
 		var msisdn_key : Long = _; 
 		var tbl_dt : Int = _; 
 		var original_timestamp : String = _; 
 		var value_of_pack : String = _; 
 		var name_of_pack : String = _; 
 		var vas_event_type_identifier : Int = _; 
 		var validity : Int = _; 
 		var main_balance_before_event : String = _; 
 		var main_balance_after_event : String = _; 
 		var product_id : String = _; 

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
      val fieldX = ru.typeOf[cm_vas_event].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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
   
      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container cm_vas_event", null);
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
      throw new Exception(s"$index is out of range for message cm_vas_event");
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
      var attributeVals = new Array[AttributeValue](10);
      try{
 				attributeVals(0) = new AttributeValue(this.msisdn_key, attributeTypes(0)) 
				attributeVals(1) = new AttributeValue(this.tbl_dt, attributeTypes(1)) 
				attributeVals(2) = new AttributeValue(this.original_timestamp, attributeTypes(2)) 
				attributeVals(3) = new AttributeValue(this.value_of_pack, attributeTypes(3)) 
				attributeVals(4) = new AttributeValue(this.name_of_pack, attributeTypes(4)) 
				attributeVals(5) = new AttributeValue(this.vas_event_type_identifier, attributeTypes(5)) 
				attributeVals(6) = new AttributeValue(this.validity, attributeTypes(6)) 
				attributeVals(7) = new AttributeValue(this.main_balance_before_event, attributeTypes(7)) 
				attributeVals(8) = new AttributeValue(this.main_balance_after_event, attributeTypes(8)) 
				attributeVals(9) = new AttributeValue(this.product_id, attributeTypes(9)) 
       
      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
      return attributeVals;
    }      
    
    override def getOnlyValuesForAllAttributes(): Array[Object] = {
      var allVals = new Array[Object](10);
      try{
 				allVals(0) = this.msisdn_key.asInstanceOf[AnyRef]; 
				allVals(1) = this.tbl_dt.asInstanceOf[AnyRef]; 
				allVals(2) = this.original_timestamp.asInstanceOf[AnyRef]; 
				allVals(3) = this.value_of_pack.asInstanceOf[AnyRef]; 
				allVals(4) = this.name_of_pack.asInstanceOf[AnyRef]; 
				allVals(5) = this.vas_event_type_identifier.asInstanceOf[AnyRef]; 
				allVals(6) = this.validity.asInstanceOf[AnyRef]; 
				allVals(7) = this.main_balance_before_event.asInstanceOf[AnyRef]; 
				allVals(8) = this.main_balance_after_event.asInstanceOf[AnyRef]; 
				allVals(9) = this.product_id.asInstanceOf[AnyRef]; 

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
   
  			 if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message cm_vas_event", null)
			 set(keyTypes(key).getIndex, value); 

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };
      
    }
  
      
    def set(index : Int, value :Any): Unit = {
      if (value == null) throw new Exception(s"Value is null for index $index in message cm_vas_event ")
      if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message cm_vas_event ")
      setFnArr(index)(this, value)
    }
    
    override def set(key: String, value: Any, valTyp: String) = {
      throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
    }
  
    private def fromFunc(other: cm_vas_event): cm_vas_event = {  
   			this.msisdn_key = com.ligadata.BaseTypes.LongImpl.Clone(other.msisdn_key);
			this.tbl_dt = com.ligadata.BaseTypes.IntImpl.Clone(other.tbl_dt);
			this.original_timestamp = com.ligadata.BaseTypes.StringImpl.Clone(other.original_timestamp);
			this.value_of_pack = com.ligadata.BaseTypes.StringImpl.Clone(other.value_of_pack);
			this.name_of_pack = com.ligadata.BaseTypes.StringImpl.Clone(other.name_of_pack);
			this.vas_event_type_identifier = com.ligadata.BaseTypes.IntImpl.Clone(other.vas_event_type_identifier);
			this.validity = com.ligadata.BaseTypes.IntImpl.Clone(other.validity);
			this.main_balance_before_event = com.ligadata.BaseTypes.StringImpl.Clone(other.main_balance_before_event);
			this.main_balance_after_event = com.ligadata.BaseTypes.StringImpl.Clone(other.main_balance_after_event);
			this.product_id = com.ligadata.BaseTypes.StringImpl.Clone(other.product_id);

      this.setTimePartitionData(com.ligadata.BaseTypes.LongImpl.Clone(other.getTimePartitionData));
      return this;
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
 	 def withmsisdn_key(value: Long) : cm_vas_event = {
		 this.msisdn_key = value 
		 return this 
 	 } 
	 def withtbl_dt(value: Int) : cm_vas_event = {
		 this.tbl_dt = value 
		 return this 
 	 } 
	 def withoriginal_timestamp(value: String) : cm_vas_event = {
		 this.original_timestamp = value 
		 return this 
 	 } 
	 def withvalue_of_pack(value: String) : cm_vas_event = {
		 this.value_of_pack = value 
		 return this 
 	 } 
	 def withname_of_pack(value: String) : cm_vas_event = {
		 this.name_of_pack = value 
		 return this 
 	 } 
	 def withvas_event_type_identifier(value: Int) : cm_vas_event = {
		 this.vas_event_type_identifier = value 
		 return this 
 	 } 
	 def withvalidity(value: Int) : cm_vas_event = {
		 this.validity = value 
		 return this 
 	 } 
	 def withmain_balance_before_event(value: String) : cm_vas_event = {
		 this.main_balance_before_event = value 
		 return this 
 	 } 
	 def withmain_balance_after_event(value: String) : cm_vas_event = {
		 this.main_balance_after_event = value 
		 return this 
 	 } 
	 def withproduct_id(value: String) : cm_vas_event = {
		 this.product_id = value 
		 return this 
 	 } 



    override def getRDDObject(): RDDObject[ContainerInterface] = cm_vas_event.asInstanceOf[RDDObject[ContainerInterface]];


        def isCaseSensitive(): Boolean = cm_vas_event.isCaseSensitive(); 
    def caseSensitiveKey(keyName: String): String = {
      if(isCaseSensitive)
        return keyName;
      else return keyName.toLowerCase;
    }


    
    def this(factory:MessageFactoryInterface) = {
      this(factory, null)
     }
    
    def this(other: cm_vas_event) = {
      this(other.getFactory.asInstanceOf[MessageFactoryInterface], other)
    }

}