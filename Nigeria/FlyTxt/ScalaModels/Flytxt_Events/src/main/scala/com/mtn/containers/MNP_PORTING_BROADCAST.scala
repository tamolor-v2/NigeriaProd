package com.mtn.containers

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


object MNP_PORTING_BROADCAST extends RDDObject[MNP_PORTING_BROADCAST] with ContainerFactoryInterface {

  val log = LogManager.getLogger(getClass)
        type T = MNP_PORTING_BROADCAST ;
        override def getFullTypeName: String = "com.mtn.containers.MNP_PORTING_BROADCAST";
        override def getTypeNameSpace: String = "com.mtn.containers";
        override def getTypeName: String = "MNP_PORTING_BROADCAST";
        override def getTypeVersion: String = "000000.000001.000000";
        override def getSchemaId: Int = 2005738;
        private var elementId: Long = 0L;
        override def setElementId(elemId: Long): Unit = { elementId = elemId; }
        override def getElementId: Long = elementId;
        override def getTenantId: String = "tenant1";
        override def createInstance: MNP_PORTING_BROADCAST = new MNP_PORTING_BROADCAST(MNP_PORTING_BROADCAST);
        override def isFixed: Boolean = true;
        def isCaseSensitive(): Boolean = false;
        override def getContainerType: ContainerTypes.ContainerType = ContainerTypes.ContainerType.CONTAINER
        override def getFullName = getFullTypeName;
        override def getRddTenantId = getTenantId;
        override def toJavaRDDObject: JavaRDDObject[T] = JavaRDDObject.fromRDDObject[T](this);

    def build = new T(this)
    def build(from: T) = new T(from)
   override def getPartitionKeyNames: Array[String] = Array("msisdn_key");

  override def getPrimaryKeyNames: Array[String] = Array[String]();


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

    override def getAvroSchema: String = """{ "type": "record",  "namespace" : "com.mtn.containers" , "name" : "mnp_porting_broadcast" , "fields":[{ "name" : "msisdn_key" , "type" : "long"},{ "name" : "donor_id" , "type" : "string"},{ "name" : "recipient_id" , "type" : "string"},{ "name" : "date_time_stamp" , "type" : "long"},{ "name" : "action_date" , "type" : "string"},{ "name" : "file_name" , "type" : "string"},{ "name" : "file_offset" , "type" : "long"},{ "name" : "kamanja_loaded_date" , "type" : "string"},{ "name" : "file_mod_date" , "type" : "string"},{ "name" : "date_key" , "type" : "int"},{ "name" : "event_timestamp_enrich" , "type" : "long"},{ "name" : "original_timestamp_enrich" , "type" : "string"}]}""";

      var attributeTypes = generateAttributeTypes;

    private def generateAttributeTypes(): Array[AttributeTypeInfo] = {
      var attributeTypes = new Array[AttributeTypeInfo](12);
                                                                                         attributeTypes(0) = new AttributeTypeInfo("msisdn_key", 0, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
                 attributeTypes(1) = new AttributeTypeInfo("donor_id", 1, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
                 attributeTypes(2) = new AttributeTypeInfo("recipient_id", 2, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
                 attributeTypes(3) = new AttributeTypeInfo("date_time_stamp", 3, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
                 attributeTypes(4) = new AttributeTypeInfo("action_date", 4, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
                 attributeTypes(5) = new AttributeTypeInfo("file_name", 5, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
                 attributeTypes(6) = new AttributeTypeInfo("file_offset", 6, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
                 attributeTypes(7) = new AttributeTypeInfo("kamanja_loaded_date", 7, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
                 attributeTypes(8) = new AttributeTypeInfo("file_mod_date", 8, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)
                 attributeTypes(9) = new AttributeTypeInfo("date_key", 9, AttributeTypeInfo.TypeCategory.INT, -1, -1, 0)
                 attributeTypes(10) = new AttributeTypeInfo("event_timestamp_enrich", 10, AttributeTypeInfo.TypeCategory.LONG, -1, -1, 0)
                 attributeTypes(11) = new AttributeTypeInfo("original_timestamp_enrich", 11, AttributeTypeInfo.TypeCategory.STRING, -1, -1, 0)


      return attributeTypes
    }


                 var keyTypes: Map[String, AttributeTypeInfo] = attributeTypes.map { a => (a.getName, a) }.toMap;
                                def setFn0(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[Long])
                                  curObj.msisdn_key = value.asInstanceOf[Long];
                                 else throw new Exception(s"Value is the not the correct type Long for field msisdn_key in message MNP_PORTING_BROADCAST")
                                }
                                def setFn1(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.donor_id = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field donor_id in message MNP_PORTING_BROADCAST")
                                }
                                def setFn2(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.recipient_id = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field recipient_id in message MNP_PORTING_BROADCAST")
                                }
                                def setFn3(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[Long])
                                  curObj.date_time_stamp = value.asInstanceOf[Long];
                                 else throw new Exception(s"Value is the not the correct type Long for field date_time_stamp in message MNP_PORTING_BROADCAST")
                                }
                                def setFn4(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.action_date = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field action_date in message MNP_PORTING_BROADCAST")
                                }
                                def setFn5(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.file_name = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field file_name in message MNP_PORTING_BROADCAST")
                                }
                                def setFn6(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[Long])
                                  curObj.file_offset = value.asInstanceOf[Long];
                                 else throw new Exception(s"Value is the not the correct type Long for field file_offset in message MNP_PORTING_BROADCAST")
                                }
                                def setFn7(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.kamanja_loaded_date = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field kamanja_loaded_date in message MNP_PORTING_BROADCAST")
                                }
                                def setFn8(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.file_mod_date = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field file_mod_date in message MNP_PORTING_BROADCAST")
                                }
                                def setFn9(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[Int])
                                  curObj.date_key = value.asInstanceOf[Int];
                                 else throw new Exception(s"Value is the not the correct type Int for field date_key in message MNP_PORTING_BROADCAST")
                                }
                                def setFn10(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[Long])
                                  curObj.event_timestamp_enrich = value.asInstanceOf[Long];
                                 else throw new Exception(s"Value is the not the correct type Long for field event_timestamp_enrich in message MNP_PORTING_BROADCAST")
                                }
                                def setFn11(curObj: MNP_PORTING_BROADCAST, value: Any): Unit ={
                                if(value.isInstanceOf[String])
                                  curObj.original_timestamp_enrich = value.asInstanceOf[String];
                                 else throw new Exception(s"Value is the not the correct type String for field original_timestamp_enrich in message MNP_PORTING_BROADCAST")
                                }

    val setFnArr = Array[(MNP_PORTING_BROADCAST, Any) => Unit](setFn0,setFn1,setFn2,setFn3,setFn4,setFn5,setFn6,setFn7,setFn8,setFn9,setFn10,setFn11    )
                def getFn0(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.msisdn_key.asInstanceOf[AnyRef];
                def getFn1(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.donor_id.asInstanceOf[AnyRef];
                def getFn2(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.recipient_id.asInstanceOf[AnyRef];
                def getFn3(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.date_time_stamp.asInstanceOf[AnyRef];
                def getFn4(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.action_date.asInstanceOf[AnyRef];
                def getFn5(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.file_name.asInstanceOf[AnyRef];
                def getFn6(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.file_offset.asInstanceOf[AnyRef];
                def getFn7(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.kamanja_loaded_date.asInstanceOf[AnyRef];
                def getFn8(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.file_mod_date.asInstanceOf[AnyRef];
                def getFn9(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.date_key.asInstanceOf[AnyRef];
                def getFn10(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.event_timestamp_enrich.asInstanceOf[AnyRef];
                def getFn11(curObj: MNP_PORTING_BROADCAST): AnyRef = curObj.original_timestamp_enrich.asInstanceOf[AnyRef];

    val getFnArr = Array[(MNP_PORTING_BROADCAST) => AnyRef](getFn0,getFn1,getFn2,getFn3,getFn4,getFn5,getFn6,getFn7,getFn8,getFn9,getFn10,getFn11    )



    override def getRDDObject(): RDDObject[ContainerInterface] = MNP_PORTING_BROADCAST.asInstanceOf[RDDObject[ContainerInterface]];



    final override def convertFrom(srcObj: Any): T = convertFrom(createInstance(), srcObj);

    override def convertFrom(newVerObj: Any, oldVerobj: Any): ContainerInterface = {
      try {
        if (oldVerobj == null) return null;
        oldVerobj match {

      case oldVerobj: com.mtn.containers.MNP_PORTING_BROADCAST => { return  convertToVer1000000(oldVerobj); }
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

    private def convertToVer1000000(oldVerobj: com.mtn.containers.MNP_PORTING_BROADCAST): com.mtn.containers.MNP_PORTING_BROADCAST= {
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
  override def PartitionKeyData(inputdata: InputData): Array[String] = { throw new Exception("Deprecated method PartitionKeyData in obj MNP_PORTING_BROADCAST") };
  override def PrimaryKeyData(inputdata: InputData): Array[String] = throw new Exception("Deprecated method PrimaryKeyData in obj MNP_PORTING_BROADCAST");
  override def TimePartitionData(inputdata: InputData): Long = throw new Exception("Deprecated method TimePartitionData in obj MNP_PORTING_BROADCAST");
 override def NeedToTransformData: Boolean = false
    }

class MNP_PORTING_BROADCAST(factory: ContainerFactoryInterface, other: MNP_PORTING_BROADCAST) extends ContainerInterface(factory) {

  val log = MNP_PORTING_BROADCAST.log

      var attributeTypes = MNP_PORTING_BROADCAST.attributeTypes

                 var keyTypes = MNP_PORTING_BROADCAST.keyTypes;

     if (other != null && other != this) {
      // call copying fields from other to local variables
      fromFunc(other)
    }

    override def save: Unit = { MNP_PORTING_BROADCAST.saveOne(this) }

    def Clone(): ContainerOrConcept = { MNP_PORTING_BROADCAST.build(this) }

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


      var setFnArr = MNP_PORTING_BROADCAST.setFnArr

      var getFnArr = MNP_PORTING_BROADCAST.getFnArr

                var msisdn_key : Long = _;
                var donor_id : String = _;
                var recipient_id : String = _;
                var date_time_stamp : Long = _;
                var action_date : String = _;
                var file_name : String = _;
                var file_offset : Long = _;
                var kamanja_loaded_date : String = _;
                var file_mod_date : String = _;
                var date_key : Int = _;
                var event_timestamp_enrich : Long = _;
                var original_timestamp_enrich : String = _;

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
      val fieldX = ru.typeOf[MNP_PORTING_BROADCAST].declaration(ru.newTermName(key)).asTerm.accessed.asTerm
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

      if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message/container MNP_PORTING_BROADCAST", null);
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
      throw new Exception(s"$index is out of range for message MNP_PORTING_BROADCAST");
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
      var attributeVals = new Array[AttributeValue](12);
      try{
                                attributeVals(0) = new AttributeValue(this.msisdn_key, attributeTypes(0))
                                attributeVals(1) = new AttributeValue(this.donor_id, attributeTypes(1))
                                attributeVals(2) = new AttributeValue(this.recipient_id, attributeTypes(2))
                                attributeVals(3) = new AttributeValue(this.date_time_stamp, attributeTypes(3))
                                attributeVals(4) = new AttributeValue(this.action_date, attributeTypes(4))
                                attributeVals(5) = new AttributeValue(this.file_name, attributeTypes(5))
                                attributeVals(6) = new AttributeValue(this.file_offset, attributeTypes(6))
                                attributeVals(7) = new AttributeValue(this.kamanja_loaded_date, attributeTypes(7))
                                attributeVals(8) = new AttributeValue(this.file_mod_date, attributeTypes(8))
                                attributeVals(9) = new AttributeValue(this.date_key, attributeTypes(9))
                                attributeVals(10) = new AttributeValue(this.event_timestamp_enrich, attributeTypes(10))
                                attributeVals(11) = new AttributeValue(this.original_timestamp_enrich, attributeTypes(11))

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };

      return attributeVals;
    }

    override def getOnlyValuesForAllAttributes(): Array[Object] = {
      var allVals = new Array[Object](12);
      try{
                                allVals(0) = this.msisdn_key.asInstanceOf[AnyRef];
                                allVals(1) = this.donor_id.asInstanceOf[AnyRef];
                                allVals(2) = this.recipient_id.asInstanceOf[AnyRef];
                                allVals(3) = this.date_time_stamp.asInstanceOf[AnyRef];
                                allVals(4) = this.action_date.asInstanceOf[AnyRef];
                                allVals(5) = this.file_name.asInstanceOf[AnyRef];
                                allVals(6) = this.file_offset.asInstanceOf[AnyRef];
                                allVals(7) = this.kamanja_loaded_date.asInstanceOf[AnyRef];
                                allVals(8) = this.file_mod_date.asInstanceOf[AnyRef];
                                allVals(9) = this.date_key.asInstanceOf[AnyRef];
                                allVals(10) = this.event_timestamp_enrich.asInstanceOf[AnyRef];
                                allVals(11) = this.original_timestamp_enrich.asInstanceOf[AnyRef];

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

                         if (!keyTypes.contains(key)) throw new KeyNotFoundException(s"Key $key does not exists in message MNP_PORTING_BROADCAST", null)
                         set(keyTypes(key).getIndex, value);

      }catch {
          case e: Exception => {
          log.debug("", e)
          throw e
        }
      };

    }


    def set(index : Int, value :Any): Unit = {
      if (value == null) throw new Exception(s"Value is null for index $index in message MNP_PORTING_BROADCAST ")
      if (index < 0 || index >= setFnArr.length) throw new Exception(s"$index is out of range for message MNP_PORTING_BROADCAST ")
      setFnArr(index)(this, value)
    }

    override def set(key: String, value: Any, valTyp: String) = {
      throw new Exception ("Set Func for Value and ValueType By Key is not supported for Fixed Messages" )
    }

    private def fromFunc(other: MNP_PORTING_BROADCAST): MNP_PORTING_BROADCAST = {
                        this.msisdn_key = com.ligadata.BaseTypes.LongImpl.Clone(other.msisdn_key);
                        this.donor_id = com.ligadata.BaseTypes.StringImpl.Clone(other.donor_id);
                        this.recipient_id = com.ligadata.BaseTypes.StringImpl.Clone(other.recipient_id);
                        this.date_time_stamp = com.ligadata.BaseTypes.LongImpl.Clone(other.date_time_stamp);
                        this.action_date = com.ligadata.BaseTypes.StringImpl.Clone(other.action_date);
                        this.file_name = com.ligadata.BaseTypes.StringImpl.Clone(other.file_name);
                        this.file_offset = com.ligadata.BaseTypes.LongImpl.Clone(other.file_offset);
                        this.kamanja_loaded_date = com.ligadata.BaseTypes.StringImpl.Clone(other.kamanja_loaded_date);
                        this.file_mod_date = com.ligadata.BaseTypes.StringImpl.Clone(other.file_mod_date);
                        this.date_key = com.ligadata.BaseTypes.IntImpl.Clone(other.date_key);
                        this.event_timestamp_enrich = com.ligadata.BaseTypes.LongImpl.Clone(other.event_timestamp_enrich);
                        this.original_timestamp_enrich = com.ligadata.BaseTypes.StringImpl.Clone(other.original_timestamp_enrich);

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
         def withmsisdn_key(value: Long) : MNP_PORTING_BROADCAST = {
                 this.msisdn_key = value
                 return this
         }
         def withdonor_id(value: String) : MNP_PORTING_BROADCAST = {
                 this.donor_id = value
                 return this
         }
         def withrecipient_id(value: String) : MNP_PORTING_BROADCAST = {
                 this.recipient_id = value
                 return this
         }
         def withdate_time_stamp(value: Long) : MNP_PORTING_BROADCAST = {
                 this.date_time_stamp = value
                 return this
         }
         def withaction_date(value: String) : MNP_PORTING_BROADCAST = {
                 this.action_date = value
                 return this
         }
         def withfile_name(value: String) : MNP_PORTING_BROADCAST = {
                 this.file_name = value
                 return this
         }
         def withfile_offset(value: Long) : MNP_PORTING_BROADCAST = {
                 this.file_offset = value
                 return this
         }
         def withkamanja_loaded_date(value: String) : MNP_PORTING_BROADCAST = {
                 this.kamanja_loaded_date = value
                 return this
         }
         def withfile_mod_date(value: String) : MNP_PORTING_BROADCAST = {
                 this.file_mod_date = value
                 return this
         }
         def withdate_key(value: Int) : MNP_PORTING_BROADCAST = {
                 this.date_key = value
                 return this
         }
         def withevent_timestamp_enrich(value: Long) : MNP_PORTING_BROADCAST = {
                 this.event_timestamp_enrich = value
                 return this
         }
         def withoriginal_timestamp_enrich(value: String) : MNP_PORTING_BROADCAST = {
                 this.original_timestamp_enrich = value
                 return this
         }



    override def getRDDObject(): RDDObject[ContainerInterface] = MNP_PORTING_BROADCAST.asInstanceOf[RDDObject[ContainerInterface]];


        def isCaseSensitive(): Boolean = MNP_PORTING_BROADCAST.isCaseSensitive();
    def caseSensitiveKey(keyName: String): String = {
      if(isCaseSensitive)
        return keyName;
      else return keyName.toLowerCase;
    }



    def this(factory:ContainerFactoryInterface) = {
      this(factory, null)
     }

    def this(other: MNP_PORTING_BROADCAST) = {
      this(other.getFactory.asInstanceOf[ContainerFactoryInterface], other)
    }

}