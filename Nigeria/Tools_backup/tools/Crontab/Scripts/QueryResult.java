// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue May 22 04:27:35 WAT 2018
// For connector: org.apache.sqoop.manager.OracleManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("KEY_VALUE_1_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        KEY_VALUE_1_V = (String)value;
      }
    });
    setters.put("OFFER_DESC_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OFFER_DESC_V = (String)value;
      }
    });
    setters.put("OFFER_CODE_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OFFER_CODE_V = (String)value;
      }
    });
    setters.put("RECORD_MODULE_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RECORD_MODULE_V = (String)value;
      }
    });
    setters.put("ACCOUNT_NUMBER_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ACCOUNT_NUMBER_V = (String)value;
      }
    });
    setters.put("SUBSCRIBER_NUMBER_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SUBSCRIBER_NUMBER_V = (String)value;
      }
    });
    setters.put("ACCOUNT_NAME_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ACCOUNT_NAME_V = (String)value;
      }
    });
    setters.put("TIMESTAMP_D", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIMESTAMP_D = (java.sql.Timestamp)value;
      }
    });
    setters.put("AMOUNT_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AMOUNT_V = (java.math.BigDecimal)value;
      }
    });
    setters.put("PREACT_REASON_CODE_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PREACT_REASON_CODE_V = (String)value;
      }
    });
    setters.put("SERVICE_CLASS_ID_V", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SERVICE_CLASS_ID_V = (String)value;
      }
    });
  }
  public QueryResult() {
    init0();
  }
  private String KEY_VALUE_1_V;
  public String get_KEY_VALUE_1_V() {
    return KEY_VALUE_1_V;
  }
  public void set_KEY_VALUE_1_V(String KEY_VALUE_1_V) {
    this.KEY_VALUE_1_V = KEY_VALUE_1_V;
  }
  public QueryResult with_KEY_VALUE_1_V(String KEY_VALUE_1_V) {
    this.KEY_VALUE_1_V = KEY_VALUE_1_V;
    return this;
  }
  private String OFFER_DESC_V;
  public String get_OFFER_DESC_V() {
    return OFFER_DESC_V;
  }
  public void set_OFFER_DESC_V(String OFFER_DESC_V) {
    this.OFFER_DESC_V = OFFER_DESC_V;
  }
  public QueryResult with_OFFER_DESC_V(String OFFER_DESC_V) {
    this.OFFER_DESC_V = OFFER_DESC_V;
    return this;
  }
  private String OFFER_CODE_V;
  public String get_OFFER_CODE_V() {
    return OFFER_CODE_V;
  }
  public void set_OFFER_CODE_V(String OFFER_CODE_V) {
    this.OFFER_CODE_V = OFFER_CODE_V;
  }
  public QueryResult with_OFFER_CODE_V(String OFFER_CODE_V) {
    this.OFFER_CODE_V = OFFER_CODE_V;
    return this;
  }
  private String RECORD_MODULE_V;
  public String get_RECORD_MODULE_V() {
    return RECORD_MODULE_V;
  }
  public void set_RECORD_MODULE_V(String RECORD_MODULE_V) {
    this.RECORD_MODULE_V = RECORD_MODULE_V;
  }
  public QueryResult with_RECORD_MODULE_V(String RECORD_MODULE_V) {
    this.RECORD_MODULE_V = RECORD_MODULE_V;
    return this;
  }
  private String ACCOUNT_NUMBER_V;
  public String get_ACCOUNT_NUMBER_V() {
    return ACCOUNT_NUMBER_V;
  }
  public void set_ACCOUNT_NUMBER_V(String ACCOUNT_NUMBER_V) {
    this.ACCOUNT_NUMBER_V = ACCOUNT_NUMBER_V;
  }
  public QueryResult with_ACCOUNT_NUMBER_V(String ACCOUNT_NUMBER_V) {
    this.ACCOUNT_NUMBER_V = ACCOUNT_NUMBER_V;
    return this;
  }
  private String SUBSCRIBER_NUMBER_V;
  public String get_SUBSCRIBER_NUMBER_V() {
    return SUBSCRIBER_NUMBER_V;
  }
  public void set_SUBSCRIBER_NUMBER_V(String SUBSCRIBER_NUMBER_V) {
    this.SUBSCRIBER_NUMBER_V = SUBSCRIBER_NUMBER_V;
  }
  public QueryResult with_SUBSCRIBER_NUMBER_V(String SUBSCRIBER_NUMBER_V) {
    this.SUBSCRIBER_NUMBER_V = SUBSCRIBER_NUMBER_V;
    return this;
  }
  private String ACCOUNT_NAME_V;
  public String get_ACCOUNT_NAME_V() {
    return ACCOUNT_NAME_V;
  }
  public void set_ACCOUNT_NAME_V(String ACCOUNT_NAME_V) {
    this.ACCOUNT_NAME_V = ACCOUNT_NAME_V;
  }
  public QueryResult with_ACCOUNT_NAME_V(String ACCOUNT_NAME_V) {
    this.ACCOUNT_NAME_V = ACCOUNT_NAME_V;
    return this;
  }
  private java.sql.Timestamp TIMESTAMP_D;
  public java.sql.Timestamp get_TIMESTAMP_D() {
    return TIMESTAMP_D;
  }
  public void set_TIMESTAMP_D(java.sql.Timestamp TIMESTAMP_D) {
    this.TIMESTAMP_D = TIMESTAMP_D;
  }
  public QueryResult with_TIMESTAMP_D(java.sql.Timestamp TIMESTAMP_D) {
    this.TIMESTAMP_D = TIMESTAMP_D;
    return this;
  }
  private java.math.BigDecimal AMOUNT_V;
  public java.math.BigDecimal get_AMOUNT_V() {
    return AMOUNT_V;
  }
  public void set_AMOUNT_V(java.math.BigDecimal AMOUNT_V) {
    this.AMOUNT_V = AMOUNT_V;
  }
  public QueryResult with_AMOUNT_V(java.math.BigDecimal AMOUNT_V) {
    this.AMOUNT_V = AMOUNT_V;
    return this;
  }
  private String PREACT_REASON_CODE_V;
  public String get_PREACT_REASON_CODE_V() {
    return PREACT_REASON_CODE_V;
  }
  public void set_PREACT_REASON_CODE_V(String PREACT_REASON_CODE_V) {
    this.PREACT_REASON_CODE_V = PREACT_REASON_CODE_V;
  }
  public QueryResult with_PREACT_REASON_CODE_V(String PREACT_REASON_CODE_V) {
    this.PREACT_REASON_CODE_V = PREACT_REASON_CODE_V;
    return this;
  }
  private String SERVICE_CLASS_ID_V;
  public String get_SERVICE_CLASS_ID_V() {
    return SERVICE_CLASS_ID_V;
  }
  public void set_SERVICE_CLASS_ID_V(String SERVICE_CLASS_ID_V) {
    this.SERVICE_CLASS_ID_V = SERVICE_CLASS_ID_V;
  }
  public QueryResult with_SERVICE_CLASS_ID_V(String SERVICE_CLASS_ID_V) {
    this.SERVICE_CLASS_ID_V = SERVICE_CLASS_ID_V;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.KEY_VALUE_1_V == null ? that.KEY_VALUE_1_V == null : this.KEY_VALUE_1_V.equals(that.KEY_VALUE_1_V));
    equal = equal && (this.OFFER_DESC_V == null ? that.OFFER_DESC_V == null : this.OFFER_DESC_V.equals(that.OFFER_DESC_V));
    equal = equal && (this.OFFER_CODE_V == null ? that.OFFER_CODE_V == null : this.OFFER_CODE_V.equals(that.OFFER_CODE_V));
    equal = equal && (this.RECORD_MODULE_V == null ? that.RECORD_MODULE_V == null : this.RECORD_MODULE_V.equals(that.RECORD_MODULE_V));
    equal = equal && (this.ACCOUNT_NUMBER_V == null ? that.ACCOUNT_NUMBER_V == null : this.ACCOUNT_NUMBER_V.equals(that.ACCOUNT_NUMBER_V));
    equal = equal && (this.SUBSCRIBER_NUMBER_V == null ? that.SUBSCRIBER_NUMBER_V == null : this.SUBSCRIBER_NUMBER_V.equals(that.SUBSCRIBER_NUMBER_V));
    equal = equal && (this.ACCOUNT_NAME_V == null ? that.ACCOUNT_NAME_V == null : this.ACCOUNT_NAME_V.equals(that.ACCOUNT_NAME_V));
    equal = equal && (this.TIMESTAMP_D == null ? that.TIMESTAMP_D == null : this.TIMESTAMP_D.equals(that.TIMESTAMP_D));
    equal = equal && (this.AMOUNT_V == null ? that.AMOUNT_V == null : this.AMOUNT_V.equals(that.AMOUNT_V));
    equal = equal && (this.PREACT_REASON_CODE_V == null ? that.PREACT_REASON_CODE_V == null : this.PREACT_REASON_CODE_V.equals(that.PREACT_REASON_CODE_V));
    equal = equal && (this.SERVICE_CLASS_ID_V == null ? that.SERVICE_CLASS_ID_V == null : this.SERVICE_CLASS_ID_V.equals(that.SERVICE_CLASS_ID_V));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.KEY_VALUE_1_V == null ? that.KEY_VALUE_1_V == null : this.KEY_VALUE_1_V.equals(that.KEY_VALUE_1_V));
    equal = equal && (this.OFFER_DESC_V == null ? that.OFFER_DESC_V == null : this.OFFER_DESC_V.equals(that.OFFER_DESC_V));
    equal = equal && (this.OFFER_CODE_V == null ? that.OFFER_CODE_V == null : this.OFFER_CODE_V.equals(that.OFFER_CODE_V));
    equal = equal && (this.RECORD_MODULE_V == null ? that.RECORD_MODULE_V == null : this.RECORD_MODULE_V.equals(that.RECORD_MODULE_V));
    equal = equal && (this.ACCOUNT_NUMBER_V == null ? that.ACCOUNT_NUMBER_V == null : this.ACCOUNT_NUMBER_V.equals(that.ACCOUNT_NUMBER_V));
    equal = equal && (this.SUBSCRIBER_NUMBER_V == null ? that.SUBSCRIBER_NUMBER_V == null : this.SUBSCRIBER_NUMBER_V.equals(that.SUBSCRIBER_NUMBER_V));
    equal = equal && (this.ACCOUNT_NAME_V == null ? that.ACCOUNT_NAME_V == null : this.ACCOUNT_NAME_V.equals(that.ACCOUNT_NAME_V));
    equal = equal && (this.TIMESTAMP_D == null ? that.TIMESTAMP_D == null : this.TIMESTAMP_D.equals(that.TIMESTAMP_D));
    equal = equal && (this.AMOUNT_V == null ? that.AMOUNT_V == null : this.AMOUNT_V.equals(that.AMOUNT_V));
    equal = equal && (this.PREACT_REASON_CODE_V == null ? that.PREACT_REASON_CODE_V == null : this.PREACT_REASON_CODE_V.equals(that.PREACT_REASON_CODE_V));
    equal = equal && (this.SERVICE_CLASS_ID_V == null ? that.SERVICE_CLASS_ID_V == null : this.SERVICE_CLASS_ID_V.equals(that.SERVICE_CLASS_ID_V));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.KEY_VALUE_1_V = JdbcWritableBridge.readString(1, __dbResults);
    this.OFFER_DESC_V = JdbcWritableBridge.readString(2, __dbResults);
    this.OFFER_CODE_V = JdbcWritableBridge.readString(3, __dbResults);
    this.RECORD_MODULE_V = JdbcWritableBridge.readString(4, __dbResults);
    this.ACCOUNT_NUMBER_V = JdbcWritableBridge.readString(5, __dbResults);
    this.SUBSCRIBER_NUMBER_V = JdbcWritableBridge.readString(6, __dbResults);
    this.ACCOUNT_NAME_V = JdbcWritableBridge.readString(7, __dbResults);
    this.TIMESTAMP_D = JdbcWritableBridge.readTimestamp(8, __dbResults);
    this.AMOUNT_V = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.PREACT_REASON_CODE_V = JdbcWritableBridge.readString(10, __dbResults);
    this.SERVICE_CLASS_ID_V = JdbcWritableBridge.readString(11, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.KEY_VALUE_1_V = JdbcWritableBridge.readString(1, __dbResults);
    this.OFFER_DESC_V = JdbcWritableBridge.readString(2, __dbResults);
    this.OFFER_CODE_V = JdbcWritableBridge.readString(3, __dbResults);
    this.RECORD_MODULE_V = JdbcWritableBridge.readString(4, __dbResults);
    this.ACCOUNT_NUMBER_V = JdbcWritableBridge.readString(5, __dbResults);
    this.SUBSCRIBER_NUMBER_V = JdbcWritableBridge.readString(6, __dbResults);
    this.ACCOUNT_NAME_V = JdbcWritableBridge.readString(7, __dbResults);
    this.TIMESTAMP_D = JdbcWritableBridge.readTimestamp(8, __dbResults);
    this.AMOUNT_V = JdbcWritableBridge.readBigDecimal(9, __dbResults);
    this.PREACT_REASON_CODE_V = JdbcWritableBridge.readString(10, __dbResults);
    this.SERVICE_CLASS_ID_V = JdbcWritableBridge.readString(11, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(KEY_VALUE_1_V, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OFFER_DESC_V, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OFFER_CODE_V, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RECORD_MODULE_V, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ACCOUNT_NUMBER_V, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SUBSCRIBER_NUMBER_V, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ACCOUNT_NAME_V, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(TIMESTAMP_D, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(AMOUNT_V, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PREACT_REASON_CODE_V, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SERVICE_CLASS_ID_V, 11 + __off, 12, __dbStmt);
    return 11;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(KEY_VALUE_1_V, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OFFER_DESC_V, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OFFER_CODE_V, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RECORD_MODULE_V, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ACCOUNT_NUMBER_V, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SUBSCRIBER_NUMBER_V, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ACCOUNT_NAME_V, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(TIMESTAMP_D, 8 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(AMOUNT_V, 9 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(PREACT_REASON_CODE_V, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(SERVICE_CLASS_ID_V, 11 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.KEY_VALUE_1_V = null;
    } else {
    this.KEY_VALUE_1_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OFFER_DESC_V = null;
    } else {
    this.OFFER_DESC_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OFFER_CODE_V = null;
    } else {
    this.OFFER_CODE_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RECORD_MODULE_V = null;
    } else {
    this.RECORD_MODULE_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ACCOUNT_NUMBER_V = null;
    } else {
    this.ACCOUNT_NUMBER_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SUBSCRIBER_NUMBER_V = null;
    } else {
    this.SUBSCRIBER_NUMBER_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ACCOUNT_NAME_V = null;
    } else {
    this.ACCOUNT_NAME_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIMESTAMP_D = null;
    } else {
    this.TIMESTAMP_D = new Timestamp(__dataIn.readLong());
    this.TIMESTAMP_D.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.AMOUNT_V = null;
    } else {
    this.AMOUNT_V = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PREACT_REASON_CODE_V = null;
    } else {
    this.PREACT_REASON_CODE_V = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SERVICE_CLASS_ID_V = null;
    } else {
    this.SERVICE_CLASS_ID_V = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.KEY_VALUE_1_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, KEY_VALUE_1_V);
    }
    if (null == this.OFFER_DESC_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OFFER_DESC_V);
    }
    if (null == this.OFFER_CODE_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OFFER_CODE_V);
    }
    if (null == this.RECORD_MODULE_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECORD_MODULE_V);
    }
    if (null == this.ACCOUNT_NUMBER_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ACCOUNT_NUMBER_V);
    }
    if (null == this.SUBSCRIBER_NUMBER_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SUBSCRIBER_NUMBER_V);
    }
    if (null == this.ACCOUNT_NAME_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ACCOUNT_NAME_V);
    }
    if (null == this.TIMESTAMP_D) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.TIMESTAMP_D.getTime());
    __dataOut.writeInt(this.TIMESTAMP_D.getNanos());
    }
    if (null == this.AMOUNT_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.AMOUNT_V, __dataOut);
    }
    if (null == this.PREACT_REASON_CODE_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PREACT_REASON_CODE_V);
    }
    if (null == this.SERVICE_CLASS_ID_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SERVICE_CLASS_ID_V);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.KEY_VALUE_1_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, KEY_VALUE_1_V);
    }
    if (null == this.OFFER_DESC_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OFFER_DESC_V);
    }
    if (null == this.OFFER_CODE_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OFFER_CODE_V);
    }
    if (null == this.RECORD_MODULE_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECORD_MODULE_V);
    }
    if (null == this.ACCOUNT_NUMBER_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ACCOUNT_NUMBER_V);
    }
    if (null == this.SUBSCRIBER_NUMBER_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SUBSCRIBER_NUMBER_V);
    }
    if (null == this.ACCOUNT_NAME_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ACCOUNT_NAME_V);
    }
    if (null == this.TIMESTAMP_D) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.TIMESTAMP_D.getTime());
    __dataOut.writeInt(this.TIMESTAMP_D.getNanos());
    }
    if (null == this.AMOUNT_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.AMOUNT_V, __dataOut);
    }
    if (null == this.PREACT_REASON_CODE_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PREACT_REASON_CODE_V);
    }
    if (null == this.SERVICE_CLASS_ID_V) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SERVICE_CLASS_ID_V);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 124, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(KEY_VALUE_1_V==null?"null":KEY_VALUE_1_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OFFER_DESC_V==null?"null":OFFER_DESC_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OFFER_CODE_V==null?"null":OFFER_CODE_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECORD_MODULE_V==null?"null":RECORD_MODULE_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACCOUNT_NUMBER_V==null?"null":ACCOUNT_NUMBER_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SUBSCRIBER_NUMBER_V==null?"null":SUBSCRIBER_NUMBER_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACCOUNT_NAME_V==null?"null":ACCOUNT_NAME_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIMESTAMP_D==null?"null":"" + TIMESTAMP_D, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AMOUNT_V==null?"null":AMOUNT_V.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PREACT_REASON_CODE_V==null?"null":PREACT_REASON_CODE_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SERVICE_CLASS_ID_V==null?"null":SERVICE_CLASS_ID_V, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(KEY_VALUE_1_V==null?"null":KEY_VALUE_1_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OFFER_DESC_V==null?"null":OFFER_DESC_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OFFER_CODE_V==null?"null":OFFER_CODE_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECORD_MODULE_V==null?"null":RECORD_MODULE_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACCOUNT_NUMBER_V==null?"null":ACCOUNT_NUMBER_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SUBSCRIBER_NUMBER_V==null?"null":SUBSCRIBER_NUMBER_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACCOUNT_NAME_V==null?"null":ACCOUNT_NAME_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIMESTAMP_D==null?"null":"" + TIMESTAMP_D, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AMOUNT_V==null?"null":AMOUNT_V.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PREACT_REASON_CODE_V==null?"null":PREACT_REASON_CODE_V, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SERVICE_CLASS_ID_V==null?"null":SERVICE_CLASS_ID_V, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 124, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.KEY_VALUE_1_V = null; } else {
      this.KEY_VALUE_1_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OFFER_DESC_V = null; } else {
      this.OFFER_DESC_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OFFER_CODE_V = null; } else {
      this.OFFER_CODE_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECORD_MODULE_V = null; } else {
      this.RECORD_MODULE_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ACCOUNT_NUMBER_V = null; } else {
      this.ACCOUNT_NUMBER_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SUBSCRIBER_NUMBER_V = null; } else {
      this.SUBSCRIBER_NUMBER_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ACCOUNT_NAME_V = null; } else {
      this.ACCOUNT_NAME_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TIMESTAMP_D = null; } else {
      this.TIMESTAMP_D = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AMOUNT_V = null; } else {
      this.AMOUNT_V = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PREACT_REASON_CODE_V = null; } else {
      this.PREACT_REASON_CODE_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SERVICE_CLASS_ID_V = null; } else {
      this.SERVICE_CLASS_ID_V = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.KEY_VALUE_1_V = null; } else {
      this.KEY_VALUE_1_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OFFER_DESC_V = null; } else {
      this.OFFER_DESC_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OFFER_CODE_V = null; } else {
      this.OFFER_CODE_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECORD_MODULE_V = null; } else {
      this.RECORD_MODULE_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ACCOUNT_NUMBER_V = null; } else {
      this.ACCOUNT_NUMBER_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SUBSCRIBER_NUMBER_V = null; } else {
      this.SUBSCRIBER_NUMBER_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ACCOUNT_NAME_V = null; } else {
      this.ACCOUNT_NAME_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TIMESTAMP_D = null; } else {
      this.TIMESTAMP_D = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AMOUNT_V = null; } else {
      this.AMOUNT_V = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PREACT_REASON_CODE_V = null; } else {
      this.PREACT_REASON_CODE_V = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SERVICE_CLASS_ID_V = null; } else {
      this.SERVICE_CLASS_ID_V = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.TIMESTAMP_D = (o.TIMESTAMP_D != null) ? (java.sql.Timestamp) o.TIMESTAMP_D.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.TIMESTAMP_D = (o.TIMESTAMP_D != null) ? (java.sql.Timestamp) o.TIMESTAMP_D.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("KEY_VALUE_1_V", this.KEY_VALUE_1_V);
    __sqoop$field_map.put("OFFER_DESC_V", this.OFFER_DESC_V);
    __sqoop$field_map.put("OFFER_CODE_V", this.OFFER_CODE_V);
    __sqoop$field_map.put("RECORD_MODULE_V", this.RECORD_MODULE_V);
    __sqoop$field_map.put("ACCOUNT_NUMBER_V", this.ACCOUNT_NUMBER_V);
    __sqoop$field_map.put("SUBSCRIBER_NUMBER_V", this.SUBSCRIBER_NUMBER_V);
    __sqoop$field_map.put("ACCOUNT_NAME_V", this.ACCOUNT_NAME_V);
    __sqoop$field_map.put("TIMESTAMP_D", this.TIMESTAMP_D);
    __sqoop$field_map.put("AMOUNT_V", this.AMOUNT_V);
    __sqoop$field_map.put("PREACT_REASON_CODE_V", this.PREACT_REASON_CODE_V);
    __sqoop$field_map.put("SERVICE_CLASS_ID_V", this.SERVICE_CLASS_ID_V);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("KEY_VALUE_1_V", this.KEY_VALUE_1_V);
    __sqoop$field_map.put("OFFER_DESC_V", this.OFFER_DESC_V);
    __sqoop$field_map.put("OFFER_CODE_V", this.OFFER_CODE_V);
    __sqoop$field_map.put("RECORD_MODULE_V", this.RECORD_MODULE_V);
    __sqoop$field_map.put("ACCOUNT_NUMBER_V", this.ACCOUNT_NUMBER_V);
    __sqoop$field_map.put("SUBSCRIBER_NUMBER_V", this.SUBSCRIBER_NUMBER_V);
    __sqoop$field_map.put("ACCOUNT_NAME_V", this.ACCOUNT_NAME_V);
    __sqoop$field_map.put("TIMESTAMP_D", this.TIMESTAMP_D);
    __sqoop$field_map.put("AMOUNT_V", this.AMOUNT_V);
    __sqoop$field_map.put("PREACT_REASON_CODE_V", this.PREACT_REASON_CODE_V);
    __sqoop$field_map.put("SERVICE_CLASS_ID_V", this.SERVICE_CLASS_ID_V);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
