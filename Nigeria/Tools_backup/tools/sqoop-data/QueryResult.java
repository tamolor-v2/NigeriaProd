// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Mar 21 09:17:24 WAT 2018
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
    setters.put("CDR_FILE_NO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CDR_FILE_NO = (java.math.BigDecimal)value;
      }
    });
    setters.put("CDR_UCI_NO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CDR_UCI_NO = (java.math.BigDecimal)value;
      }
    });
    setters.put("CDR_UCI_ELEMENT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CDR_UCI_ELEMENT = (java.math.BigDecimal)value;
      }
    });
    setters.put("CDR_KEY_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CDR_KEY_ID = (String)value;
      }
    });
    setters.put("CALL_SCENARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CALL_SCENARIO = (String)value;
      }
    });
    setters.put("REMUNERATION_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REMUNERATION_INDICATOR = (String)value;
      }
    });
    setters.put("BILLING_BASE_DIRECTION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_BASE_DIRECTION = (String)value;
      }
    });
    setters.put("FRANCHISE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FRANCHISE = (String)value;
      }
    });
    setters.put("PRODUCT_GROUP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PRODUCT_GROUP = (String)value;
      }
    });
    setters.put("REVERSE_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVERSE_INDICATOR = (String)value;
      }
    });
    setters.put("EVENT_DIRECTION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EVENT_DIRECTION = (String)value;
      }
    });
    setters.put("DERIVED_EVENT_DIRECTION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DERIVED_EVENT_DIRECTION = (String)value;
      }
    });
    setters.put("BILLING_RATING_SCENARIO_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_RATING_SCENARIO_TYPE = (String)value;
      }
    });
    setters.put("BILLED_PRODUCT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLED_PRODUCT = (String)value;
      }
    });
    setters.put("RATING_SCENARIO", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATING_SCENARIO = (String)value;
      }
    });
    setters.put("RATING_RULE_DEPENDENCY_IND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATING_RULE_DEPENDENCY_IND = (String)value;
      }
    });
    setters.put("ESTIMATE_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ESTIMATE_INDICATOR = (String)value;
      }
    });
    setters.put("ACCOUNTING_METHOD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ACCOUNTING_METHOD = (String)value;
      }
    });
    setters.put("CASH_FLOW", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CASH_FLOW = (String)value;
      }
    });
    setters.put("COMPONENT_DIRECTION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COMPONENT_DIRECTION = (String)value;
      }
    });
    setters.put("STATEMENT_DIRECTION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        STATEMENT_DIRECTION = (String)value;
      }
    });
    setters.put("BILLING_METHOD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_METHOD = (String)value;
      }
    });
    setters.put("RATING_COMPONENT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATING_COMPONENT = (String)value;
      }
    });
    setters.put("CALL_ATTEMPT_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CALL_ATTEMPT_INDICATOR = (String)value;
      }
    });
    setters.put("PARAMETER_MATRIX", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PARAMETER_MATRIX = (String)value;
      }
    });
    setters.put("PARAMETER_MATRIX_LINE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PARAMETER_MATRIX_LINE = (java.math.BigDecimal)value;
      }
    });
    setters.put("STEP_NUMBER", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        STEP_NUMBER = (java.math.BigDecimal)value;
      }
    });
    setters.put("RATE_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATE_TYPE = (String)value;
      }
    });
    setters.put("TIME_PREMIUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIME_PREMIUM = (String)value;
      }
    });
    setters.put("BILLING_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_OPERATOR = (String)value;
      }
    });
    setters.put("RATE_OWNER_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATE_OWNER_OPERATOR = (String)value;
      }
    });
    setters.put("SETTLEMENT_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SETTLEMENT_OPERATOR = (String)value;
      }
    });
    setters.put("RATE_NAME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATE_NAME = (String)value;
      }
    });
    setters.put("TIER", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIER = (String)value;
      }
    });
    setters.put("TIER_GROUP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIER_GROUP = (String)value;
      }
    });
    setters.put("TIER_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIER_TYPE = (String)value;
      }
    });
    setters.put("OPTIMUM_POI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OPTIMUM_POI = (String)value;
      }
    });
    setters.put("ADHOC_SUMMARY_IND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ADHOC_SUMMARY_IND = (String)value;
      }
    });
    setters.put("BILLING_SUMMARY_IND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_SUMMARY_IND = (String)value;
      }
    });
    setters.put("COMPLEMENTARY_SUMMARY_IND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        COMPLEMENTARY_SUMMARY_IND = (String)value;
      }
    });
    setters.put("TRAFFIC_SUMMARY_IND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_SUMMARY_IND = (String)value;
      }
    });
    setters.put("MINIMUM_CHARGE_IND", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MINIMUM_CHARGE_IND = (String)value;
      }
    });
    setters.put("CALL_DURATION_BUCKET", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CALL_DURATION_BUCKET = (java.math.BigDecimal)value;
      }
    });
    setters.put("TIME_OF_DAY_BUCKET", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TIME_OF_DAY_BUCKET = (java.math.BigDecimal)value;
      }
    });
    setters.put("USER_SUMMARISATION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        USER_SUMMARISATION = (String)value;
      }
    });
    setters.put("INCOMING_NODE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        INCOMING_NODE = (String)value;
      }
    });
    setters.put("INCOMING_PRODUCT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        INCOMING_PRODUCT = (String)value;
      }
    });
    setters.put("INCOMING_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        INCOMING_OPERATOR = (String)value;
      }
    });
    setters.put("INCOMING_PATH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        INCOMING_PATH = (String)value;
      }
    });
    setters.put("INCOMING_POI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        INCOMING_POI = (String)value;
      }
    });
    setters.put("OUTGOING_NODE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OUTGOING_NODE = (String)value;
      }
    });
    setters.put("OUTGOING_PRODUCT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OUTGOING_PRODUCT = (String)value;
      }
    });
    setters.put("OUTGOING_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OUTGOING_OPERATOR = (String)value;
      }
    });
    setters.put("OUTGOING_PATH", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OUTGOING_PATH = (String)value;
      }
    });
    setters.put("OUTGOING_POI", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        OUTGOING_POI = (String)value;
      }
    });
    setters.put("TRAFFIC_RATING_SCENARIO_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_RATING_SCENARIO_TYPE = (String)value;
      }
    });
    setters.put("TRAFFIC_ROUTE_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_ROUTE_TYPE = (String)value;
      }
    });
    setters.put("TRAFFIC_ROUTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_ROUTE = (String)value;
      }
    });
    setters.put("TRAFFIC_AGREEMENT_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_AGREEMENT_OPERATOR = (String)value;
      }
    });
    setters.put("TRAFFIC_NEGOTIATION_DIR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_NEGOTIATION_DIR = (String)value;
      }
    });
    setters.put("BILLING_ROUTE_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_ROUTE_TYPE = (String)value;
      }
    });
    setters.put("BILLING_ROUTE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_ROUTE = (String)value;
      }
    });
    setters.put("BILLING_AGREEMENT_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_AGREEMENT_OPERATOR = (String)value;
      }
    });
    setters.put("BILLING_NEGOTIATION_DIR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_NEGOTIATION_DIR = (String)value;
      }
    });
    setters.put("ROUTE_IDENTIFIER", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ROUTE_IDENTIFIER = (String)value;
      }
    });
    setters.put("REFILE_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REFILE_INDICATOR = (String)value;
      }
    });
    setters.put("AGREEMENT_NAAG_ANUM_LEVEL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AGREEMENT_NAAG_ANUM_LEVEL = (String)value;
      }
    });
    setters.put("AGREEMENT_NAAG_ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AGREEMENT_NAAG_ANUM = (String)value;
      }
    });
    setters.put("AGREEMENT_NAAG_BNUM_LEVEL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AGREEMENT_NAAG_BNUM_LEVEL = (String)value;
      }
    });
    setters.put("AGREEMENT_NAAG_BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AGREEMENT_NAAG_BNUM = (String)value;
      }
    });
    setters.put("WORLD_VIEW_NAAG_ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        WORLD_VIEW_NAAG_ANUM = (String)value;
      }
    });
    setters.put("WORLD_VIEW_ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        WORLD_VIEW_ANUM = (String)value;
      }
    });
    setters.put("WORLD_VIEW_NAAG_BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        WORLD_VIEW_NAAG_BNUM = (String)value;
      }
    });
    setters.put("WORLD_VIEW_BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        WORLD_VIEW_BNUM = (String)value;
      }
    });
    setters.put("TRAFFIC_BASE_DIRECTION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_BASE_DIRECTION = (String)value;
      }
    });
    setters.put("DATA_UNIT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_UNIT = (String)value;
      }
    });
    setters.put("DATA_UNIT_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_UNIT_2 = (String)value;
      }
    });
    setters.put("DATA_UNIT_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_UNIT_3 = (String)value;
      }
    });
    setters.put("DATA_UNIT_4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_UNIT_4 = (String)value;
      }
    });
    setters.put("DISCRETE_RATING_PARAMETER_1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DISCRETE_RATING_PARAMETER_1 = (String)value;
      }
    });
    setters.put("DISCRETE_RATING_PARAMETER_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DISCRETE_RATING_PARAMETER_2 = (String)value;
      }
    });
    setters.put("DISCRETE_RATING_PARAMETER_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DISCRETE_RATING_PARAMETER_3 = (String)value;
      }
    });
    setters.put("REVENUE_SHARE_CURRENCY_1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVENUE_SHARE_CURRENCY_1 = (String)value;
      }
    });
    setters.put("REVENUE_SHARE_CURRENCY_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVENUE_SHARE_CURRENCY_2 = (String)value;
      }
    });
    setters.put("REVENUE_SHARE_CURRENCY_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVENUE_SHARE_CURRENCY_3 = (String)value;
      }
    });
    setters.put("ANUM_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ANUM_OPERATOR = (String)value;
      }
    });
    setters.put("ANUM_CNP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ANUM_CNP = (String)value;
      }
    });
    setters.put("TRAFFIC_NAAG", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_NAAG = (String)value;
      }
    });
    setters.put("NAAG_ANUM_LEVEL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NAAG_ANUM_LEVEL = (String)value;
      }
    });
    setters.put("RECON_NAAG_ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RECON_NAAG_ANUM = (String)value;
      }
    });
    setters.put("NETWORK_ADDRESS_AGGR_ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_ADDRESS_AGGR_ANUM = (String)value;
      }
    });
    setters.put("NETWORK_TYPE_ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_TYPE_ANUM = (String)value;
      }
    });
    setters.put("BNUM_OPERATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BNUM_OPERATOR = (String)value;
      }
    });
    setters.put("BNUM_CNP", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BNUM_CNP = (String)value;
      }
    });
    setters.put("NAAG_BNUM_LEVEL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NAAG_BNUM_LEVEL = (String)value;
      }
    });
    setters.put("NETWORK_TYPE_BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_TYPE_BNUM = (String)value;
      }
    });
    setters.put("RECON_NAAG_BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RECON_NAAG_BNUM = (String)value;
      }
    });
    setters.put("NETWORK_ADDRESS_AGGR_BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_ADDRESS_AGGR_BNUM = (String)value;
      }
    });
    setters.put("DERIVED_PRODUCT_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DERIVED_PRODUCT_INDICATOR = (String)value;
      }
    });
    setters.put("ANUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ANUM = (String)value;
      }
    });
    setters.put("BNUM", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BNUM = (String)value;
      }
    });
    setters.put("RECORD_SEQUENCE_NUMBER", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RECORD_SEQUENCE_NUMBER = (String)value;
      }
    });
    setters.put("USER_DATA", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        USER_DATA = (String)value;
      }
    });
    setters.put("USER_DATA_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        USER_DATA_2 = (String)value;
      }
    });
    setters.put("USER_DATA_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        USER_DATA_3 = (String)value;
      }
    });
    setters.put("CALL_COUNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CALL_COUNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("START_CALL_COUNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        START_CALL_COUNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("RATE_STEP_CALL_COUNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATE_STEP_CALL_COUNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("APPORTIONED_CALL_COUNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        APPORTIONED_CALL_COUNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("APPORTIONED_DURATION_SECONDS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        APPORTIONED_DURATION_SECONDS = (java.math.BigDecimal)value;
      }
    });
    setters.put("ACTUAL_USAGE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ACTUAL_USAGE = (java.math.BigDecimal)value;
      }
    });
    setters.put("CHARGED_USAGE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHARGED_USAGE = (java.math.BigDecimal)value;
      }
    });
    setters.put("CHARGED_UNITS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHARGED_UNITS = (String)value;
      }
    });
    setters.put("EVENT_DURATION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EVENT_DURATION = (java.math.BigDecimal)value;
      }
    });
    setters.put("MINIMUM_CHARGE_ADJUSTMENT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MINIMUM_CHARGE_ADJUSTMENT = (java.math.BigDecimal)value;
      }
    });
    setters.put("MAXIMUM_CHARGE_ADJUSTMENT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MAXIMUM_CHARGE_ADJUSTMENT = (java.math.BigDecimal)value;
      }
    });
    setters.put("NETWORK_DURATION", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_DURATION = (java.math.BigDecimal)value;
      }
    });
    setters.put("DATA_VOLUME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_VOLUME = (java.math.BigDecimal)value;
      }
    });
    setters.put("DATA_VOLUME_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_VOLUME_2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("DATA_VOLUME_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_VOLUME_3 = (java.math.BigDecimal)value;
      }
    });
    setters.put("DATA_VOLUME_4", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DATA_VOLUME_4 = (java.math.BigDecimal)value;
      }
    });
    setters.put("REVENUE_SHARE_AMOUNT_1", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVENUE_SHARE_AMOUNT_1 = (java.math.BigDecimal)value;
      }
    });
    setters.put("REVENUE_SHARE_AMOUNT_2", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVENUE_SHARE_AMOUNT_2 = (java.math.BigDecimal)value;
      }
    });
    setters.put("REVENUE_SHARE_AMOUNT_3", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REVENUE_SHARE_AMOUNT_3 = (java.math.BigDecimal)value;
      }
    });
    setters.put("BASE_AMOUNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BASE_AMOUNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("AMOUNT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AMOUNT = (java.math.BigDecimal)value;
      }
    });
    setters.put("DLYS_DETAIL_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        DLYS_DETAIL_ID = (String)value;
      }
    });
    setters.put("TRAFFIC_PERIOD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_PERIOD = (String)value;
      }
    });
    setters.put("MESSAGE_DATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MESSAGE_DATE = (java.sql.Timestamp)value;
      }
    });
    setters.put("BILLING_DATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_DATE = (java.sql.Timestamp)value;
      }
    });
    setters.put("ADJUSTED_DATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ADJUSTED_DATE = (java.sql.Timestamp)value;
      }
    });
    setters.put("PROCESS_DATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        PROCESS_DATE = (java.sql.Timestamp)value;
      }
    });
    setters.put("EVENT_START_DATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EVENT_START_DATE = (java.sql.Timestamp)value;
      }
    });
    setters.put("EVENT_START_TIME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EVENT_START_TIME = (String)value;
      }
    });
    setters.put("NETWORK_START_DATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_START_DATE = (java.math.BigDecimal)value;
      }
    });
    setters.put("NETWORK_START_TIME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        NETWORK_START_TIME = (java.math.BigDecimal)value;
      }
    });
    setters.put("BILLING_START_TIME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_START_TIME = (java.math.BigDecimal)value;
      }
    });
    setters.put("BILLING_END_TIME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BILLING_END_TIME = (java.math.BigDecimal)value;
      }
    });
    setters.put("FLAT_RATE_CHARGE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FLAT_RATE_CHARGE = (String)value;
      }
    });
    setters.put("RATE_STEP_FLAT_CHARGE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATE_STEP_FLAT_CHARGE = (java.math.BigDecimal)value;
      }
    });
    setters.put("UNIT_COST_USED", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        UNIT_COST_USED = (java.math.BigDecimal)value;
      }
    });
    setters.put("CHARGE_BUNDLE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CHARGE_BUNDLE = (java.math.BigDecimal)value;
      }
    });
    setters.put("BASE_AMOUNT_FACTOR_USED", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BASE_AMOUNT_FACTOR_USED = (java.math.BigDecimal)value;
      }
    });
    setters.put("REFUND_FACTOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REFUND_FACTOR = (java.math.BigDecimal)value;
      }
    });
    setters.put("CURRENCY", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CURRENCY = (String)value;
      }
    });
    setters.put("CURRENCY_CONVERSION_RATE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CURRENCY_CONVERSION_RATE = (java.math.BigDecimal)value;
      }
    });
    setters.put("BASE_UNIT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        BASE_UNIT = (String)value;
      }
    });
    setters.put("RATE_UNIT", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATE_UNIT = (String)value;
      }
    });
    setters.put("ROUNDED_UNIT_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        ROUNDED_UNIT_ID = (java.math.BigDecimal)value;
      }
    });
    setters.put("RECORD_TYPE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RECORD_TYPE = (java.math.BigDecimal)value;
      }
    });
    setters.put("LINK_FIELD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        LINK_FIELD = (java.math.BigDecimal)value;
      }
    });
    setters.put("REASON_FOR_CLEARDOWN", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REASON_FOR_CLEARDOWN = (java.math.BigDecimal)value;
      }
    });
    setters.put("REPAIR_INDICATOR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        REPAIR_INDICATOR = (java.math.BigDecimal)value;
      }
    });
    setters.put("RATED_BILLING_PERIOD", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RATED_BILLING_PERIOD = (java.math.BigDecimal)value;
      }
    });
    setters.put("TRAFFIC_MOVEMENT_CTR", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TRAFFIC_MOVEMENT_CTR = (java.math.BigDecimal)value;
      }
    });
    setters.put("RTE_LOOKUP_STYLE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        RTE_LOOKUP_STYLE = (String)value;
      }
    });
    setters.put("CDR_SOURCE", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CDR_SOURCE = (String)value;
      }
    });
    setters.put("CARRIER_DESTINATION_NAME", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CARRIER_DESTINATION_NAME = (String)value;
      }
    });
    setters.put("CARRIER_DESTINATION_ALIAS", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CARRIER_DESTINATION_ALIAS = (String)value;
      }
    });
  }
  public QueryResult() {
    init0();
  }
  private java.math.BigDecimal CDR_FILE_NO;
  public java.math.BigDecimal get_CDR_FILE_NO() {
    return CDR_FILE_NO;
  }
  public void set_CDR_FILE_NO(java.math.BigDecimal CDR_FILE_NO) {
    this.CDR_FILE_NO = CDR_FILE_NO;
  }
  public QueryResult with_CDR_FILE_NO(java.math.BigDecimal CDR_FILE_NO) {
    this.CDR_FILE_NO = CDR_FILE_NO;
    return this;
  }
  private java.math.BigDecimal CDR_UCI_NO;
  public java.math.BigDecimal get_CDR_UCI_NO() {
    return CDR_UCI_NO;
  }
  public void set_CDR_UCI_NO(java.math.BigDecimal CDR_UCI_NO) {
    this.CDR_UCI_NO = CDR_UCI_NO;
  }
  public QueryResult with_CDR_UCI_NO(java.math.BigDecimal CDR_UCI_NO) {
    this.CDR_UCI_NO = CDR_UCI_NO;
    return this;
  }
  private java.math.BigDecimal CDR_UCI_ELEMENT;
  public java.math.BigDecimal get_CDR_UCI_ELEMENT() {
    return CDR_UCI_ELEMENT;
  }
  public void set_CDR_UCI_ELEMENT(java.math.BigDecimal CDR_UCI_ELEMENT) {
    this.CDR_UCI_ELEMENT = CDR_UCI_ELEMENT;
  }
  public QueryResult with_CDR_UCI_ELEMENT(java.math.BigDecimal CDR_UCI_ELEMENT) {
    this.CDR_UCI_ELEMENT = CDR_UCI_ELEMENT;
    return this;
  }
  private String CDR_KEY_ID;
  public String get_CDR_KEY_ID() {
    return CDR_KEY_ID;
  }
  public void set_CDR_KEY_ID(String CDR_KEY_ID) {
    this.CDR_KEY_ID = CDR_KEY_ID;
  }
  public QueryResult with_CDR_KEY_ID(String CDR_KEY_ID) {
    this.CDR_KEY_ID = CDR_KEY_ID;
    return this;
  }
  private String CALL_SCENARIO;
  public String get_CALL_SCENARIO() {
    return CALL_SCENARIO;
  }
  public void set_CALL_SCENARIO(String CALL_SCENARIO) {
    this.CALL_SCENARIO = CALL_SCENARIO;
  }
  public QueryResult with_CALL_SCENARIO(String CALL_SCENARIO) {
    this.CALL_SCENARIO = CALL_SCENARIO;
    return this;
  }
  private String REMUNERATION_INDICATOR;
  public String get_REMUNERATION_INDICATOR() {
    return REMUNERATION_INDICATOR;
  }
  public void set_REMUNERATION_INDICATOR(String REMUNERATION_INDICATOR) {
    this.REMUNERATION_INDICATOR = REMUNERATION_INDICATOR;
  }
  public QueryResult with_REMUNERATION_INDICATOR(String REMUNERATION_INDICATOR) {
    this.REMUNERATION_INDICATOR = REMUNERATION_INDICATOR;
    return this;
  }
  private String BILLING_BASE_DIRECTION;
  public String get_BILLING_BASE_DIRECTION() {
    return BILLING_BASE_DIRECTION;
  }
  public void set_BILLING_BASE_DIRECTION(String BILLING_BASE_DIRECTION) {
    this.BILLING_BASE_DIRECTION = BILLING_BASE_DIRECTION;
  }
  public QueryResult with_BILLING_BASE_DIRECTION(String BILLING_BASE_DIRECTION) {
    this.BILLING_BASE_DIRECTION = BILLING_BASE_DIRECTION;
    return this;
  }
  private String FRANCHISE;
  public String get_FRANCHISE() {
    return FRANCHISE;
  }
  public void set_FRANCHISE(String FRANCHISE) {
    this.FRANCHISE = FRANCHISE;
  }
  public QueryResult with_FRANCHISE(String FRANCHISE) {
    this.FRANCHISE = FRANCHISE;
    return this;
  }
  private String PRODUCT_GROUP;
  public String get_PRODUCT_GROUP() {
    return PRODUCT_GROUP;
  }
  public void set_PRODUCT_GROUP(String PRODUCT_GROUP) {
    this.PRODUCT_GROUP = PRODUCT_GROUP;
  }
  public QueryResult with_PRODUCT_GROUP(String PRODUCT_GROUP) {
    this.PRODUCT_GROUP = PRODUCT_GROUP;
    return this;
  }
  private String REVERSE_INDICATOR;
  public String get_REVERSE_INDICATOR() {
    return REVERSE_INDICATOR;
  }
  public void set_REVERSE_INDICATOR(String REVERSE_INDICATOR) {
    this.REVERSE_INDICATOR = REVERSE_INDICATOR;
  }
  public QueryResult with_REVERSE_INDICATOR(String REVERSE_INDICATOR) {
    this.REVERSE_INDICATOR = REVERSE_INDICATOR;
    return this;
  }
  private String EVENT_DIRECTION;
  public String get_EVENT_DIRECTION() {
    return EVENT_DIRECTION;
  }
  public void set_EVENT_DIRECTION(String EVENT_DIRECTION) {
    this.EVENT_DIRECTION = EVENT_DIRECTION;
  }
  public QueryResult with_EVENT_DIRECTION(String EVENT_DIRECTION) {
    this.EVENT_DIRECTION = EVENT_DIRECTION;
    return this;
  }
  private String DERIVED_EVENT_DIRECTION;
  public String get_DERIVED_EVENT_DIRECTION() {
    return DERIVED_EVENT_DIRECTION;
  }
  public void set_DERIVED_EVENT_DIRECTION(String DERIVED_EVENT_DIRECTION) {
    this.DERIVED_EVENT_DIRECTION = DERIVED_EVENT_DIRECTION;
  }
  public QueryResult with_DERIVED_EVENT_DIRECTION(String DERIVED_EVENT_DIRECTION) {
    this.DERIVED_EVENT_DIRECTION = DERIVED_EVENT_DIRECTION;
    return this;
  }
  private String BILLING_RATING_SCENARIO_TYPE;
  public String get_BILLING_RATING_SCENARIO_TYPE() {
    return BILLING_RATING_SCENARIO_TYPE;
  }
  public void set_BILLING_RATING_SCENARIO_TYPE(String BILLING_RATING_SCENARIO_TYPE) {
    this.BILLING_RATING_SCENARIO_TYPE = BILLING_RATING_SCENARIO_TYPE;
  }
  public QueryResult with_BILLING_RATING_SCENARIO_TYPE(String BILLING_RATING_SCENARIO_TYPE) {
    this.BILLING_RATING_SCENARIO_TYPE = BILLING_RATING_SCENARIO_TYPE;
    return this;
  }
  private String BILLED_PRODUCT;
  public String get_BILLED_PRODUCT() {
    return BILLED_PRODUCT;
  }
  public void set_BILLED_PRODUCT(String BILLED_PRODUCT) {
    this.BILLED_PRODUCT = BILLED_PRODUCT;
  }
  public QueryResult with_BILLED_PRODUCT(String BILLED_PRODUCT) {
    this.BILLED_PRODUCT = BILLED_PRODUCT;
    return this;
  }
  private String RATING_SCENARIO;
  public String get_RATING_SCENARIO() {
    return RATING_SCENARIO;
  }
  public void set_RATING_SCENARIO(String RATING_SCENARIO) {
    this.RATING_SCENARIO = RATING_SCENARIO;
  }
  public QueryResult with_RATING_SCENARIO(String RATING_SCENARIO) {
    this.RATING_SCENARIO = RATING_SCENARIO;
    return this;
  }
  private String RATING_RULE_DEPENDENCY_IND;
  public String get_RATING_RULE_DEPENDENCY_IND() {
    return RATING_RULE_DEPENDENCY_IND;
  }
  public void set_RATING_RULE_DEPENDENCY_IND(String RATING_RULE_DEPENDENCY_IND) {
    this.RATING_RULE_DEPENDENCY_IND = RATING_RULE_DEPENDENCY_IND;
  }
  public QueryResult with_RATING_RULE_DEPENDENCY_IND(String RATING_RULE_DEPENDENCY_IND) {
    this.RATING_RULE_DEPENDENCY_IND = RATING_RULE_DEPENDENCY_IND;
    return this;
  }
  private String ESTIMATE_INDICATOR;
  public String get_ESTIMATE_INDICATOR() {
    return ESTIMATE_INDICATOR;
  }
  public void set_ESTIMATE_INDICATOR(String ESTIMATE_INDICATOR) {
    this.ESTIMATE_INDICATOR = ESTIMATE_INDICATOR;
  }
  public QueryResult with_ESTIMATE_INDICATOR(String ESTIMATE_INDICATOR) {
    this.ESTIMATE_INDICATOR = ESTIMATE_INDICATOR;
    return this;
  }
  private String ACCOUNTING_METHOD;
  public String get_ACCOUNTING_METHOD() {
    return ACCOUNTING_METHOD;
  }
  public void set_ACCOUNTING_METHOD(String ACCOUNTING_METHOD) {
    this.ACCOUNTING_METHOD = ACCOUNTING_METHOD;
  }
  public QueryResult with_ACCOUNTING_METHOD(String ACCOUNTING_METHOD) {
    this.ACCOUNTING_METHOD = ACCOUNTING_METHOD;
    return this;
  }
  private String CASH_FLOW;
  public String get_CASH_FLOW() {
    return CASH_FLOW;
  }
  public void set_CASH_FLOW(String CASH_FLOW) {
    this.CASH_FLOW = CASH_FLOW;
  }
  public QueryResult with_CASH_FLOW(String CASH_FLOW) {
    this.CASH_FLOW = CASH_FLOW;
    return this;
  }
  private String COMPONENT_DIRECTION;
  public String get_COMPONENT_DIRECTION() {
    return COMPONENT_DIRECTION;
  }
  public void set_COMPONENT_DIRECTION(String COMPONENT_DIRECTION) {
    this.COMPONENT_DIRECTION = COMPONENT_DIRECTION;
  }
  public QueryResult with_COMPONENT_DIRECTION(String COMPONENT_DIRECTION) {
    this.COMPONENT_DIRECTION = COMPONENT_DIRECTION;
    return this;
  }
  private String STATEMENT_DIRECTION;
  public String get_STATEMENT_DIRECTION() {
    return STATEMENT_DIRECTION;
  }
  public void set_STATEMENT_DIRECTION(String STATEMENT_DIRECTION) {
    this.STATEMENT_DIRECTION = STATEMENT_DIRECTION;
  }
  public QueryResult with_STATEMENT_DIRECTION(String STATEMENT_DIRECTION) {
    this.STATEMENT_DIRECTION = STATEMENT_DIRECTION;
    return this;
  }
  private String BILLING_METHOD;
  public String get_BILLING_METHOD() {
    return BILLING_METHOD;
  }
  public void set_BILLING_METHOD(String BILLING_METHOD) {
    this.BILLING_METHOD = BILLING_METHOD;
  }
  public QueryResult with_BILLING_METHOD(String BILLING_METHOD) {
    this.BILLING_METHOD = BILLING_METHOD;
    return this;
  }
  private String RATING_COMPONENT;
  public String get_RATING_COMPONENT() {
    return RATING_COMPONENT;
  }
  public void set_RATING_COMPONENT(String RATING_COMPONENT) {
    this.RATING_COMPONENT = RATING_COMPONENT;
  }
  public QueryResult with_RATING_COMPONENT(String RATING_COMPONENT) {
    this.RATING_COMPONENT = RATING_COMPONENT;
    return this;
  }
  private String CALL_ATTEMPT_INDICATOR;
  public String get_CALL_ATTEMPT_INDICATOR() {
    return CALL_ATTEMPT_INDICATOR;
  }
  public void set_CALL_ATTEMPT_INDICATOR(String CALL_ATTEMPT_INDICATOR) {
    this.CALL_ATTEMPT_INDICATOR = CALL_ATTEMPT_INDICATOR;
  }
  public QueryResult with_CALL_ATTEMPT_INDICATOR(String CALL_ATTEMPT_INDICATOR) {
    this.CALL_ATTEMPT_INDICATOR = CALL_ATTEMPT_INDICATOR;
    return this;
  }
  private String PARAMETER_MATRIX;
  public String get_PARAMETER_MATRIX() {
    return PARAMETER_MATRIX;
  }
  public void set_PARAMETER_MATRIX(String PARAMETER_MATRIX) {
    this.PARAMETER_MATRIX = PARAMETER_MATRIX;
  }
  public QueryResult with_PARAMETER_MATRIX(String PARAMETER_MATRIX) {
    this.PARAMETER_MATRIX = PARAMETER_MATRIX;
    return this;
  }
  private java.math.BigDecimal PARAMETER_MATRIX_LINE;
  public java.math.BigDecimal get_PARAMETER_MATRIX_LINE() {
    return PARAMETER_MATRIX_LINE;
  }
  public void set_PARAMETER_MATRIX_LINE(java.math.BigDecimal PARAMETER_MATRIX_LINE) {
    this.PARAMETER_MATRIX_LINE = PARAMETER_MATRIX_LINE;
  }
  public QueryResult with_PARAMETER_MATRIX_LINE(java.math.BigDecimal PARAMETER_MATRIX_LINE) {
    this.PARAMETER_MATRIX_LINE = PARAMETER_MATRIX_LINE;
    return this;
  }
  private java.math.BigDecimal STEP_NUMBER;
  public java.math.BigDecimal get_STEP_NUMBER() {
    return STEP_NUMBER;
  }
  public void set_STEP_NUMBER(java.math.BigDecimal STEP_NUMBER) {
    this.STEP_NUMBER = STEP_NUMBER;
  }
  public QueryResult with_STEP_NUMBER(java.math.BigDecimal STEP_NUMBER) {
    this.STEP_NUMBER = STEP_NUMBER;
    return this;
  }
  private String RATE_TYPE;
  public String get_RATE_TYPE() {
    return RATE_TYPE;
  }
  public void set_RATE_TYPE(String RATE_TYPE) {
    this.RATE_TYPE = RATE_TYPE;
  }
  public QueryResult with_RATE_TYPE(String RATE_TYPE) {
    this.RATE_TYPE = RATE_TYPE;
    return this;
  }
  private String TIME_PREMIUM;
  public String get_TIME_PREMIUM() {
    return TIME_PREMIUM;
  }
  public void set_TIME_PREMIUM(String TIME_PREMIUM) {
    this.TIME_PREMIUM = TIME_PREMIUM;
  }
  public QueryResult with_TIME_PREMIUM(String TIME_PREMIUM) {
    this.TIME_PREMIUM = TIME_PREMIUM;
    return this;
  }
  private String BILLING_OPERATOR;
  public String get_BILLING_OPERATOR() {
    return BILLING_OPERATOR;
  }
  public void set_BILLING_OPERATOR(String BILLING_OPERATOR) {
    this.BILLING_OPERATOR = BILLING_OPERATOR;
  }
  public QueryResult with_BILLING_OPERATOR(String BILLING_OPERATOR) {
    this.BILLING_OPERATOR = BILLING_OPERATOR;
    return this;
  }
  private String RATE_OWNER_OPERATOR;
  public String get_RATE_OWNER_OPERATOR() {
    return RATE_OWNER_OPERATOR;
  }
  public void set_RATE_OWNER_OPERATOR(String RATE_OWNER_OPERATOR) {
    this.RATE_OWNER_OPERATOR = RATE_OWNER_OPERATOR;
  }
  public QueryResult with_RATE_OWNER_OPERATOR(String RATE_OWNER_OPERATOR) {
    this.RATE_OWNER_OPERATOR = RATE_OWNER_OPERATOR;
    return this;
  }
  private String SETTLEMENT_OPERATOR;
  public String get_SETTLEMENT_OPERATOR() {
    return SETTLEMENT_OPERATOR;
  }
  public void set_SETTLEMENT_OPERATOR(String SETTLEMENT_OPERATOR) {
    this.SETTLEMENT_OPERATOR = SETTLEMENT_OPERATOR;
  }
  public QueryResult with_SETTLEMENT_OPERATOR(String SETTLEMENT_OPERATOR) {
    this.SETTLEMENT_OPERATOR = SETTLEMENT_OPERATOR;
    return this;
  }
  private String RATE_NAME;
  public String get_RATE_NAME() {
    return RATE_NAME;
  }
  public void set_RATE_NAME(String RATE_NAME) {
    this.RATE_NAME = RATE_NAME;
  }
  public QueryResult with_RATE_NAME(String RATE_NAME) {
    this.RATE_NAME = RATE_NAME;
    return this;
  }
  private String TIER;
  public String get_TIER() {
    return TIER;
  }
  public void set_TIER(String TIER) {
    this.TIER = TIER;
  }
  public QueryResult with_TIER(String TIER) {
    this.TIER = TIER;
    return this;
  }
  private String TIER_GROUP;
  public String get_TIER_GROUP() {
    return TIER_GROUP;
  }
  public void set_TIER_GROUP(String TIER_GROUP) {
    this.TIER_GROUP = TIER_GROUP;
  }
  public QueryResult with_TIER_GROUP(String TIER_GROUP) {
    this.TIER_GROUP = TIER_GROUP;
    return this;
  }
  private String TIER_TYPE;
  public String get_TIER_TYPE() {
    return TIER_TYPE;
  }
  public void set_TIER_TYPE(String TIER_TYPE) {
    this.TIER_TYPE = TIER_TYPE;
  }
  public QueryResult with_TIER_TYPE(String TIER_TYPE) {
    this.TIER_TYPE = TIER_TYPE;
    return this;
  }
  private String OPTIMUM_POI;
  public String get_OPTIMUM_POI() {
    return OPTIMUM_POI;
  }
  public void set_OPTIMUM_POI(String OPTIMUM_POI) {
    this.OPTIMUM_POI = OPTIMUM_POI;
  }
  public QueryResult with_OPTIMUM_POI(String OPTIMUM_POI) {
    this.OPTIMUM_POI = OPTIMUM_POI;
    return this;
  }
  private String ADHOC_SUMMARY_IND;
  public String get_ADHOC_SUMMARY_IND() {
    return ADHOC_SUMMARY_IND;
  }
  public void set_ADHOC_SUMMARY_IND(String ADHOC_SUMMARY_IND) {
    this.ADHOC_SUMMARY_IND = ADHOC_SUMMARY_IND;
  }
  public QueryResult with_ADHOC_SUMMARY_IND(String ADHOC_SUMMARY_IND) {
    this.ADHOC_SUMMARY_IND = ADHOC_SUMMARY_IND;
    return this;
  }
  private String BILLING_SUMMARY_IND;
  public String get_BILLING_SUMMARY_IND() {
    return BILLING_SUMMARY_IND;
  }
  public void set_BILLING_SUMMARY_IND(String BILLING_SUMMARY_IND) {
    this.BILLING_SUMMARY_IND = BILLING_SUMMARY_IND;
  }
  public QueryResult with_BILLING_SUMMARY_IND(String BILLING_SUMMARY_IND) {
    this.BILLING_SUMMARY_IND = BILLING_SUMMARY_IND;
    return this;
  }
  private String COMPLEMENTARY_SUMMARY_IND;
  public String get_COMPLEMENTARY_SUMMARY_IND() {
    return COMPLEMENTARY_SUMMARY_IND;
  }
  public void set_COMPLEMENTARY_SUMMARY_IND(String COMPLEMENTARY_SUMMARY_IND) {
    this.COMPLEMENTARY_SUMMARY_IND = COMPLEMENTARY_SUMMARY_IND;
  }
  public QueryResult with_COMPLEMENTARY_SUMMARY_IND(String COMPLEMENTARY_SUMMARY_IND) {
    this.COMPLEMENTARY_SUMMARY_IND = COMPLEMENTARY_SUMMARY_IND;
    return this;
  }
  private String TRAFFIC_SUMMARY_IND;
  public String get_TRAFFIC_SUMMARY_IND() {
    return TRAFFIC_SUMMARY_IND;
  }
  public void set_TRAFFIC_SUMMARY_IND(String TRAFFIC_SUMMARY_IND) {
    this.TRAFFIC_SUMMARY_IND = TRAFFIC_SUMMARY_IND;
  }
  public QueryResult with_TRAFFIC_SUMMARY_IND(String TRAFFIC_SUMMARY_IND) {
    this.TRAFFIC_SUMMARY_IND = TRAFFIC_SUMMARY_IND;
    return this;
  }
  private String MINIMUM_CHARGE_IND;
  public String get_MINIMUM_CHARGE_IND() {
    return MINIMUM_CHARGE_IND;
  }
  public void set_MINIMUM_CHARGE_IND(String MINIMUM_CHARGE_IND) {
    this.MINIMUM_CHARGE_IND = MINIMUM_CHARGE_IND;
  }
  public QueryResult with_MINIMUM_CHARGE_IND(String MINIMUM_CHARGE_IND) {
    this.MINIMUM_CHARGE_IND = MINIMUM_CHARGE_IND;
    return this;
  }
  private java.math.BigDecimal CALL_DURATION_BUCKET;
  public java.math.BigDecimal get_CALL_DURATION_BUCKET() {
    return CALL_DURATION_BUCKET;
  }
  public void set_CALL_DURATION_BUCKET(java.math.BigDecimal CALL_DURATION_BUCKET) {
    this.CALL_DURATION_BUCKET = CALL_DURATION_BUCKET;
  }
  public QueryResult with_CALL_DURATION_BUCKET(java.math.BigDecimal CALL_DURATION_BUCKET) {
    this.CALL_DURATION_BUCKET = CALL_DURATION_BUCKET;
    return this;
  }
  private java.math.BigDecimal TIME_OF_DAY_BUCKET;
  public java.math.BigDecimal get_TIME_OF_DAY_BUCKET() {
    return TIME_OF_DAY_BUCKET;
  }
  public void set_TIME_OF_DAY_BUCKET(java.math.BigDecimal TIME_OF_DAY_BUCKET) {
    this.TIME_OF_DAY_BUCKET = TIME_OF_DAY_BUCKET;
  }
  public QueryResult with_TIME_OF_DAY_BUCKET(java.math.BigDecimal TIME_OF_DAY_BUCKET) {
    this.TIME_OF_DAY_BUCKET = TIME_OF_DAY_BUCKET;
    return this;
  }
  private String USER_SUMMARISATION;
  public String get_USER_SUMMARISATION() {
    return USER_SUMMARISATION;
  }
  public void set_USER_SUMMARISATION(String USER_SUMMARISATION) {
    this.USER_SUMMARISATION = USER_SUMMARISATION;
  }
  public QueryResult with_USER_SUMMARISATION(String USER_SUMMARISATION) {
    this.USER_SUMMARISATION = USER_SUMMARISATION;
    return this;
  }
  private String INCOMING_NODE;
  public String get_INCOMING_NODE() {
    return INCOMING_NODE;
  }
  public void set_INCOMING_NODE(String INCOMING_NODE) {
    this.INCOMING_NODE = INCOMING_NODE;
  }
  public QueryResult with_INCOMING_NODE(String INCOMING_NODE) {
    this.INCOMING_NODE = INCOMING_NODE;
    return this;
  }
  private String INCOMING_PRODUCT;
  public String get_INCOMING_PRODUCT() {
    return INCOMING_PRODUCT;
  }
  public void set_INCOMING_PRODUCT(String INCOMING_PRODUCT) {
    this.INCOMING_PRODUCT = INCOMING_PRODUCT;
  }
  public QueryResult with_INCOMING_PRODUCT(String INCOMING_PRODUCT) {
    this.INCOMING_PRODUCT = INCOMING_PRODUCT;
    return this;
  }
  private String INCOMING_OPERATOR;
  public String get_INCOMING_OPERATOR() {
    return INCOMING_OPERATOR;
  }
  public void set_INCOMING_OPERATOR(String INCOMING_OPERATOR) {
    this.INCOMING_OPERATOR = INCOMING_OPERATOR;
  }
  public QueryResult with_INCOMING_OPERATOR(String INCOMING_OPERATOR) {
    this.INCOMING_OPERATOR = INCOMING_OPERATOR;
    return this;
  }
  private String INCOMING_PATH;
  public String get_INCOMING_PATH() {
    return INCOMING_PATH;
  }
  public void set_INCOMING_PATH(String INCOMING_PATH) {
    this.INCOMING_PATH = INCOMING_PATH;
  }
  public QueryResult with_INCOMING_PATH(String INCOMING_PATH) {
    this.INCOMING_PATH = INCOMING_PATH;
    return this;
  }
  private String INCOMING_POI;
  public String get_INCOMING_POI() {
    return INCOMING_POI;
  }
  public void set_INCOMING_POI(String INCOMING_POI) {
    this.INCOMING_POI = INCOMING_POI;
  }
  public QueryResult with_INCOMING_POI(String INCOMING_POI) {
    this.INCOMING_POI = INCOMING_POI;
    return this;
  }
  private String OUTGOING_NODE;
  public String get_OUTGOING_NODE() {
    return OUTGOING_NODE;
  }
  public void set_OUTGOING_NODE(String OUTGOING_NODE) {
    this.OUTGOING_NODE = OUTGOING_NODE;
  }
  public QueryResult with_OUTGOING_NODE(String OUTGOING_NODE) {
    this.OUTGOING_NODE = OUTGOING_NODE;
    return this;
  }
  private String OUTGOING_PRODUCT;
  public String get_OUTGOING_PRODUCT() {
    return OUTGOING_PRODUCT;
  }
  public void set_OUTGOING_PRODUCT(String OUTGOING_PRODUCT) {
    this.OUTGOING_PRODUCT = OUTGOING_PRODUCT;
  }
  public QueryResult with_OUTGOING_PRODUCT(String OUTGOING_PRODUCT) {
    this.OUTGOING_PRODUCT = OUTGOING_PRODUCT;
    return this;
  }
  private String OUTGOING_OPERATOR;
  public String get_OUTGOING_OPERATOR() {
    return OUTGOING_OPERATOR;
  }
  public void set_OUTGOING_OPERATOR(String OUTGOING_OPERATOR) {
    this.OUTGOING_OPERATOR = OUTGOING_OPERATOR;
  }
  public QueryResult with_OUTGOING_OPERATOR(String OUTGOING_OPERATOR) {
    this.OUTGOING_OPERATOR = OUTGOING_OPERATOR;
    return this;
  }
  private String OUTGOING_PATH;
  public String get_OUTGOING_PATH() {
    return OUTGOING_PATH;
  }
  public void set_OUTGOING_PATH(String OUTGOING_PATH) {
    this.OUTGOING_PATH = OUTGOING_PATH;
  }
  public QueryResult with_OUTGOING_PATH(String OUTGOING_PATH) {
    this.OUTGOING_PATH = OUTGOING_PATH;
    return this;
  }
  private String OUTGOING_POI;
  public String get_OUTGOING_POI() {
    return OUTGOING_POI;
  }
  public void set_OUTGOING_POI(String OUTGOING_POI) {
    this.OUTGOING_POI = OUTGOING_POI;
  }
  public QueryResult with_OUTGOING_POI(String OUTGOING_POI) {
    this.OUTGOING_POI = OUTGOING_POI;
    return this;
  }
  private String TRAFFIC_RATING_SCENARIO_TYPE;
  public String get_TRAFFIC_RATING_SCENARIO_TYPE() {
    return TRAFFIC_RATING_SCENARIO_TYPE;
  }
  public void set_TRAFFIC_RATING_SCENARIO_TYPE(String TRAFFIC_RATING_SCENARIO_TYPE) {
    this.TRAFFIC_RATING_SCENARIO_TYPE = TRAFFIC_RATING_SCENARIO_TYPE;
  }
  public QueryResult with_TRAFFIC_RATING_SCENARIO_TYPE(String TRAFFIC_RATING_SCENARIO_TYPE) {
    this.TRAFFIC_RATING_SCENARIO_TYPE = TRAFFIC_RATING_SCENARIO_TYPE;
    return this;
  }
  private String TRAFFIC_ROUTE_TYPE;
  public String get_TRAFFIC_ROUTE_TYPE() {
    return TRAFFIC_ROUTE_TYPE;
  }
  public void set_TRAFFIC_ROUTE_TYPE(String TRAFFIC_ROUTE_TYPE) {
    this.TRAFFIC_ROUTE_TYPE = TRAFFIC_ROUTE_TYPE;
  }
  public QueryResult with_TRAFFIC_ROUTE_TYPE(String TRAFFIC_ROUTE_TYPE) {
    this.TRAFFIC_ROUTE_TYPE = TRAFFIC_ROUTE_TYPE;
    return this;
  }
  private String TRAFFIC_ROUTE;
  public String get_TRAFFIC_ROUTE() {
    return TRAFFIC_ROUTE;
  }
  public void set_TRAFFIC_ROUTE(String TRAFFIC_ROUTE) {
    this.TRAFFIC_ROUTE = TRAFFIC_ROUTE;
  }
  public QueryResult with_TRAFFIC_ROUTE(String TRAFFIC_ROUTE) {
    this.TRAFFIC_ROUTE = TRAFFIC_ROUTE;
    return this;
  }
  private String TRAFFIC_AGREEMENT_OPERATOR;
  public String get_TRAFFIC_AGREEMENT_OPERATOR() {
    return TRAFFIC_AGREEMENT_OPERATOR;
  }
  public void set_TRAFFIC_AGREEMENT_OPERATOR(String TRAFFIC_AGREEMENT_OPERATOR) {
    this.TRAFFIC_AGREEMENT_OPERATOR = TRAFFIC_AGREEMENT_OPERATOR;
  }
  public QueryResult with_TRAFFIC_AGREEMENT_OPERATOR(String TRAFFIC_AGREEMENT_OPERATOR) {
    this.TRAFFIC_AGREEMENT_OPERATOR = TRAFFIC_AGREEMENT_OPERATOR;
    return this;
  }
  private String TRAFFIC_NEGOTIATION_DIR;
  public String get_TRAFFIC_NEGOTIATION_DIR() {
    return TRAFFIC_NEGOTIATION_DIR;
  }
  public void set_TRAFFIC_NEGOTIATION_DIR(String TRAFFIC_NEGOTIATION_DIR) {
    this.TRAFFIC_NEGOTIATION_DIR = TRAFFIC_NEGOTIATION_DIR;
  }
  public QueryResult with_TRAFFIC_NEGOTIATION_DIR(String TRAFFIC_NEGOTIATION_DIR) {
    this.TRAFFIC_NEGOTIATION_DIR = TRAFFIC_NEGOTIATION_DIR;
    return this;
  }
  private String BILLING_ROUTE_TYPE;
  public String get_BILLING_ROUTE_TYPE() {
    return BILLING_ROUTE_TYPE;
  }
  public void set_BILLING_ROUTE_TYPE(String BILLING_ROUTE_TYPE) {
    this.BILLING_ROUTE_TYPE = BILLING_ROUTE_TYPE;
  }
  public QueryResult with_BILLING_ROUTE_TYPE(String BILLING_ROUTE_TYPE) {
    this.BILLING_ROUTE_TYPE = BILLING_ROUTE_TYPE;
    return this;
  }
  private String BILLING_ROUTE;
  public String get_BILLING_ROUTE() {
    return BILLING_ROUTE;
  }
  public void set_BILLING_ROUTE(String BILLING_ROUTE) {
    this.BILLING_ROUTE = BILLING_ROUTE;
  }
  public QueryResult with_BILLING_ROUTE(String BILLING_ROUTE) {
    this.BILLING_ROUTE = BILLING_ROUTE;
    return this;
  }
  private String BILLING_AGREEMENT_OPERATOR;
  public String get_BILLING_AGREEMENT_OPERATOR() {
    return BILLING_AGREEMENT_OPERATOR;
  }
  public void set_BILLING_AGREEMENT_OPERATOR(String BILLING_AGREEMENT_OPERATOR) {
    this.BILLING_AGREEMENT_OPERATOR = BILLING_AGREEMENT_OPERATOR;
  }
  public QueryResult with_BILLING_AGREEMENT_OPERATOR(String BILLING_AGREEMENT_OPERATOR) {
    this.BILLING_AGREEMENT_OPERATOR = BILLING_AGREEMENT_OPERATOR;
    return this;
  }
  private String BILLING_NEGOTIATION_DIR;
  public String get_BILLING_NEGOTIATION_DIR() {
    return BILLING_NEGOTIATION_DIR;
  }
  public void set_BILLING_NEGOTIATION_DIR(String BILLING_NEGOTIATION_DIR) {
    this.BILLING_NEGOTIATION_DIR = BILLING_NEGOTIATION_DIR;
  }
  public QueryResult with_BILLING_NEGOTIATION_DIR(String BILLING_NEGOTIATION_DIR) {
    this.BILLING_NEGOTIATION_DIR = BILLING_NEGOTIATION_DIR;
    return this;
  }
  private String ROUTE_IDENTIFIER;
  public String get_ROUTE_IDENTIFIER() {
    return ROUTE_IDENTIFIER;
  }
  public void set_ROUTE_IDENTIFIER(String ROUTE_IDENTIFIER) {
    this.ROUTE_IDENTIFIER = ROUTE_IDENTIFIER;
  }
  public QueryResult with_ROUTE_IDENTIFIER(String ROUTE_IDENTIFIER) {
    this.ROUTE_IDENTIFIER = ROUTE_IDENTIFIER;
    return this;
  }
  private String REFILE_INDICATOR;
  public String get_REFILE_INDICATOR() {
    return REFILE_INDICATOR;
  }
  public void set_REFILE_INDICATOR(String REFILE_INDICATOR) {
    this.REFILE_INDICATOR = REFILE_INDICATOR;
  }
  public QueryResult with_REFILE_INDICATOR(String REFILE_INDICATOR) {
    this.REFILE_INDICATOR = REFILE_INDICATOR;
    return this;
  }
  private String AGREEMENT_NAAG_ANUM_LEVEL;
  public String get_AGREEMENT_NAAG_ANUM_LEVEL() {
    return AGREEMENT_NAAG_ANUM_LEVEL;
  }
  public void set_AGREEMENT_NAAG_ANUM_LEVEL(String AGREEMENT_NAAG_ANUM_LEVEL) {
    this.AGREEMENT_NAAG_ANUM_LEVEL = AGREEMENT_NAAG_ANUM_LEVEL;
  }
  public QueryResult with_AGREEMENT_NAAG_ANUM_LEVEL(String AGREEMENT_NAAG_ANUM_LEVEL) {
    this.AGREEMENT_NAAG_ANUM_LEVEL = AGREEMENT_NAAG_ANUM_LEVEL;
    return this;
  }
  private String AGREEMENT_NAAG_ANUM;
  public String get_AGREEMENT_NAAG_ANUM() {
    return AGREEMENT_NAAG_ANUM;
  }
  public void set_AGREEMENT_NAAG_ANUM(String AGREEMENT_NAAG_ANUM) {
    this.AGREEMENT_NAAG_ANUM = AGREEMENT_NAAG_ANUM;
  }
  public QueryResult with_AGREEMENT_NAAG_ANUM(String AGREEMENT_NAAG_ANUM) {
    this.AGREEMENT_NAAG_ANUM = AGREEMENT_NAAG_ANUM;
    return this;
  }
  private String AGREEMENT_NAAG_BNUM_LEVEL;
  public String get_AGREEMENT_NAAG_BNUM_LEVEL() {
    return AGREEMENT_NAAG_BNUM_LEVEL;
  }
  public void set_AGREEMENT_NAAG_BNUM_LEVEL(String AGREEMENT_NAAG_BNUM_LEVEL) {
    this.AGREEMENT_NAAG_BNUM_LEVEL = AGREEMENT_NAAG_BNUM_LEVEL;
  }
  public QueryResult with_AGREEMENT_NAAG_BNUM_LEVEL(String AGREEMENT_NAAG_BNUM_LEVEL) {
    this.AGREEMENT_NAAG_BNUM_LEVEL = AGREEMENT_NAAG_BNUM_LEVEL;
    return this;
  }
  private String AGREEMENT_NAAG_BNUM;
  public String get_AGREEMENT_NAAG_BNUM() {
    return AGREEMENT_NAAG_BNUM;
  }
  public void set_AGREEMENT_NAAG_BNUM(String AGREEMENT_NAAG_BNUM) {
    this.AGREEMENT_NAAG_BNUM = AGREEMENT_NAAG_BNUM;
  }
  public QueryResult with_AGREEMENT_NAAG_BNUM(String AGREEMENT_NAAG_BNUM) {
    this.AGREEMENT_NAAG_BNUM = AGREEMENT_NAAG_BNUM;
    return this;
  }
  private String WORLD_VIEW_NAAG_ANUM;
  public String get_WORLD_VIEW_NAAG_ANUM() {
    return WORLD_VIEW_NAAG_ANUM;
  }
  public void set_WORLD_VIEW_NAAG_ANUM(String WORLD_VIEW_NAAG_ANUM) {
    this.WORLD_VIEW_NAAG_ANUM = WORLD_VIEW_NAAG_ANUM;
  }
  public QueryResult with_WORLD_VIEW_NAAG_ANUM(String WORLD_VIEW_NAAG_ANUM) {
    this.WORLD_VIEW_NAAG_ANUM = WORLD_VIEW_NAAG_ANUM;
    return this;
  }
  private String WORLD_VIEW_ANUM;
  public String get_WORLD_VIEW_ANUM() {
    return WORLD_VIEW_ANUM;
  }
  public void set_WORLD_VIEW_ANUM(String WORLD_VIEW_ANUM) {
    this.WORLD_VIEW_ANUM = WORLD_VIEW_ANUM;
  }
  public QueryResult with_WORLD_VIEW_ANUM(String WORLD_VIEW_ANUM) {
    this.WORLD_VIEW_ANUM = WORLD_VIEW_ANUM;
    return this;
  }
  private String WORLD_VIEW_NAAG_BNUM;
  public String get_WORLD_VIEW_NAAG_BNUM() {
    return WORLD_VIEW_NAAG_BNUM;
  }
  public void set_WORLD_VIEW_NAAG_BNUM(String WORLD_VIEW_NAAG_BNUM) {
    this.WORLD_VIEW_NAAG_BNUM = WORLD_VIEW_NAAG_BNUM;
  }
  public QueryResult with_WORLD_VIEW_NAAG_BNUM(String WORLD_VIEW_NAAG_BNUM) {
    this.WORLD_VIEW_NAAG_BNUM = WORLD_VIEW_NAAG_BNUM;
    return this;
  }
  private String WORLD_VIEW_BNUM;
  public String get_WORLD_VIEW_BNUM() {
    return WORLD_VIEW_BNUM;
  }
  public void set_WORLD_VIEW_BNUM(String WORLD_VIEW_BNUM) {
    this.WORLD_VIEW_BNUM = WORLD_VIEW_BNUM;
  }
  public QueryResult with_WORLD_VIEW_BNUM(String WORLD_VIEW_BNUM) {
    this.WORLD_VIEW_BNUM = WORLD_VIEW_BNUM;
    return this;
  }
  private String TRAFFIC_BASE_DIRECTION;
  public String get_TRAFFIC_BASE_DIRECTION() {
    return TRAFFIC_BASE_DIRECTION;
  }
  public void set_TRAFFIC_BASE_DIRECTION(String TRAFFIC_BASE_DIRECTION) {
    this.TRAFFIC_BASE_DIRECTION = TRAFFIC_BASE_DIRECTION;
  }
  public QueryResult with_TRAFFIC_BASE_DIRECTION(String TRAFFIC_BASE_DIRECTION) {
    this.TRAFFIC_BASE_DIRECTION = TRAFFIC_BASE_DIRECTION;
    return this;
  }
  private String DATA_UNIT;
  public String get_DATA_UNIT() {
    return DATA_UNIT;
  }
  public void set_DATA_UNIT(String DATA_UNIT) {
    this.DATA_UNIT = DATA_UNIT;
  }
  public QueryResult with_DATA_UNIT(String DATA_UNIT) {
    this.DATA_UNIT = DATA_UNIT;
    return this;
  }
  private String DATA_UNIT_2;
  public String get_DATA_UNIT_2() {
    return DATA_UNIT_2;
  }
  public void set_DATA_UNIT_2(String DATA_UNIT_2) {
    this.DATA_UNIT_2 = DATA_UNIT_2;
  }
  public QueryResult with_DATA_UNIT_2(String DATA_UNIT_2) {
    this.DATA_UNIT_2 = DATA_UNIT_2;
    return this;
  }
  private String DATA_UNIT_3;
  public String get_DATA_UNIT_3() {
    return DATA_UNIT_3;
  }
  public void set_DATA_UNIT_3(String DATA_UNIT_3) {
    this.DATA_UNIT_3 = DATA_UNIT_3;
  }
  public QueryResult with_DATA_UNIT_3(String DATA_UNIT_3) {
    this.DATA_UNIT_3 = DATA_UNIT_3;
    return this;
  }
  private String DATA_UNIT_4;
  public String get_DATA_UNIT_4() {
    return DATA_UNIT_4;
  }
  public void set_DATA_UNIT_4(String DATA_UNIT_4) {
    this.DATA_UNIT_4 = DATA_UNIT_4;
  }
  public QueryResult with_DATA_UNIT_4(String DATA_UNIT_4) {
    this.DATA_UNIT_4 = DATA_UNIT_4;
    return this;
  }
  private String DISCRETE_RATING_PARAMETER_1;
  public String get_DISCRETE_RATING_PARAMETER_1() {
    return DISCRETE_RATING_PARAMETER_1;
  }
  public void set_DISCRETE_RATING_PARAMETER_1(String DISCRETE_RATING_PARAMETER_1) {
    this.DISCRETE_RATING_PARAMETER_1 = DISCRETE_RATING_PARAMETER_1;
  }
  public QueryResult with_DISCRETE_RATING_PARAMETER_1(String DISCRETE_RATING_PARAMETER_1) {
    this.DISCRETE_RATING_PARAMETER_1 = DISCRETE_RATING_PARAMETER_1;
    return this;
  }
  private String DISCRETE_RATING_PARAMETER_2;
  public String get_DISCRETE_RATING_PARAMETER_2() {
    return DISCRETE_RATING_PARAMETER_2;
  }
  public void set_DISCRETE_RATING_PARAMETER_2(String DISCRETE_RATING_PARAMETER_2) {
    this.DISCRETE_RATING_PARAMETER_2 = DISCRETE_RATING_PARAMETER_2;
  }
  public QueryResult with_DISCRETE_RATING_PARAMETER_2(String DISCRETE_RATING_PARAMETER_2) {
    this.DISCRETE_RATING_PARAMETER_2 = DISCRETE_RATING_PARAMETER_2;
    return this;
  }
  private String DISCRETE_RATING_PARAMETER_3;
  public String get_DISCRETE_RATING_PARAMETER_3() {
    return DISCRETE_RATING_PARAMETER_3;
  }
  public void set_DISCRETE_RATING_PARAMETER_3(String DISCRETE_RATING_PARAMETER_3) {
    this.DISCRETE_RATING_PARAMETER_3 = DISCRETE_RATING_PARAMETER_3;
  }
  public QueryResult with_DISCRETE_RATING_PARAMETER_3(String DISCRETE_RATING_PARAMETER_3) {
    this.DISCRETE_RATING_PARAMETER_3 = DISCRETE_RATING_PARAMETER_3;
    return this;
  }
  private String REVENUE_SHARE_CURRENCY_1;
  public String get_REVENUE_SHARE_CURRENCY_1() {
    return REVENUE_SHARE_CURRENCY_1;
  }
  public void set_REVENUE_SHARE_CURRENCY_1(String REVENUE_SHARE_CURRENCY_1) {
    this.REVENUE_SHARE_CURRENCY_1 = REVENUE_SHARE_CURRENCY_1;
  }
  public QueryResult with_REVENUE_SHARE_CURRENCY_1(String REVENUE_SHARE_CURRENCY_1) {
    this.REVENUE_SHARE_CURRENCY_1 = REVENUE_SHARE_CURRENCY_1;
    return this;
  }
  private String REVENUE_SHARE_CURRENCY_2;
  public String get_REVENUE_SHARE_CURRENCY_2() {
    return REVENUE_SHARE_CURRENCY_2;
  }
  public void set_REVENUE_SHARE_CURRENCY_2(String REVENUE_SHARE_CURRENCY_2) {
    this.REVENUE_SHARE_CURRENCY_2 = REVENUE_SHARE_CURRENCY_2;
  }
  public QueryResult with_REVENUE_SHARE_CURRENCY_2(String REVENUE_SHARE_CURRENCY_2) {
    this.REVENUE_SHARE_CURRENCY_2 = REVENUE_SHARE_CURRENCY_2;
    return this;
  }
  private String REVENUE_SHARE_CURRENCY_3;
  public String get_REVENUE_SHARE_CURRENCY_3() {
    return REVENUE_SHARE_CURRENCY_3;
  }
  public void set_REVENUE_SHARE_CURRENCY_3(String REVENUE_SHARE_CURRENCY_3) {
    this.REVENUE_SHARE_CURRENCY_3 = REVENUE_SHARE_CURRENCY_3;
  }
  public QueryResult with_REVENUE_SHARE_CURRENCY_3(String REVENUE_SHARE_CURRENCY_3) {
    this.REVENUE_SHARE_CURRENCY_3 = REVENUE_SHARE_CURRENCY_3;
    return this;
  }
  private String ANUM_OPERATOR;
  public String get_ANUM_OPERATOR() {
    return ANUM_OPERATOR;
  }
  public void set_ANUM_OPERATOR(String ANUM_OPERATOR) {
    this.ANUM_OPERATOR = ANUM_OPERATOR;
  }
  public QueryResult with_ANUM_OPERATOR(String ANUM_OPERATOR) {
    this.ANUM_OPERATOR = ANUM_OPERATOR;
    return this;
  }
  private String ANUM_CNP;
  public String get_ANUM_CNP() {
    return ANUM_CNP;
  }
  public void set_ANUM_CNP(String ANUM_CNP) {
    this.ANUM_CNP = ANUM_CNP;
  }
  public QueryResult with_ANUM_CNP(String ANUM_CNP) {
    this.ANUM_CNP = ANUM_CNP;
    return this;
  }
  private String TRAFFIC_NAAG;
  public String get_TRAFFIC_NAAG() {
    return TRAFFIC_NAAG;
  }
  public void set_TRAFFIC_NAAG(String TRAFFIC_NAAG) {
    this.TRAFFIC_NAAG = TRAFFIC_NAAG;
  }
  public QueryResult with_TRAFFIC_NAAG(String TRAFFIC_NAAG) {
    this.TRAFFIC_NAAG = TRAFFIC_NAAG;
    return this;
  }
  private String NAAG_ANUM_LEVEL;
  public String get_NAAG_ANUM_LEVEL() {
    return NAAG_ANUM_LEVEL;
  }
  public void set_NAAG_ANUM_LEVEL(String NAAG_ANUM_LEVEL) {
    this.NAAG_ANUM_LEVEL = NAAG_ANUM_LEVEL;
  }
  public QueryResult with_NAAG_ANUM_LEVEL(String NAAG_ANUM_LEVEL) {
    this.NAAG_ANUM_LEVEL = NAAG_ANUM_LEVEL;
    return this;
  }
  private String RECON_NAAG_ANUM;
  public String get_RECON_NAAG_ANUM() {
    return RECON_NAAG_ANUM;
  }
  public void set_RECON_NAAG_ANUM(String RECON_NAAG_ANUM) {
    this.RECON_NAAG_ANUM = RECON_NAAG_ANUM;
  }
  public QueryResult with_RECON_NAAG_ANUM(String RECON_NAAG_ANUM) {
    this.RECON_NAAG_ANUM = RECON_NAAG_ANUM;
    return this;
  }
  private String NETWORK_ADDRESS_AGGR_ANUM;
  public String get_NETWORK_ADDRESS_AGGR_ANUM() {
    return NETWORK_ADDRESS_AGGR_ANUM;
  }
  public void set_NETWORK_ADDRESS_AGGR_ANUM(String NETWORK_ADDRESS_AGGR_ANUM) {
    this.NETWORK_ADDRESS_AGGR_ANUM = NETWORK_ADDRESS_AGGR_ANUM;
  }
  public QueryResult with_NETWORK_ADDRESS_AGGR_ANUM(String NETWORK_ADDRESS_AGGR_ANUM) {
    this.NETWORK_ADDRESS_AGGR_ANUM = NETWORK_ADDRESS_AGGR_ANUM;
    return this;
  }
  private String NETWORK_TYPE_ANUM;
  public String get_NETWORK_TYPE_ANUM() {
    return NETWORK_TYPE_ANUM;
  }
  public void set_NETWORK_TYPE_ANUM(String NETWORK_TYPE_ANUM) {
    this.NETWORK_TYPE_ANUM = NETWORK_TYPE_ANUM;
  }
  public QueryResult with_NETWORK_TYPE_ANUM(String NETWORK_TYPE_ANUM) {
    this.NETWORK_TYPE_ANUM = NETWORK_TYPE_ANUM;
    return this;
  }
  private String BNUM_OPERATOR;
  public String get_BNUM_OPERATOR() {
    return BNUM_OPERATOR;
  }
  public void set_BNUM_OPERATOR(String BNUM_OPERATOR) {
    this.BNUM_OPERATOR = BNUM_OPERATOR;
  }
  public QueryResult with_BNUM_OPERATOR(String BNUM_OPERATOR) {
    this.BNUM_OPERATOR = BNUM_OPERATOR;
    return this;
  }
  private String BNUM_CNP;
  public String get_BNUM_CNP() {
    return BNUM_CNP;
  }
  public void set_BNUM_CNP(String BNUM_CNP) {
    this.BNUM_CNP = BNUM_CNP;
  }
  public QueryResult with_BNUM_CNP(String BNUM_CNP) {
    this.BNUM_CNP = BNUM_CNP;
    return this;
  }
  private String NAAG_BNUM_LEVEL;
  public String get_NAAG_BNUM_LEVEL() {
    return NAAG_BNUM_LEVEL;
  }
  public void set_NAAG_BNUM_LEVEL(String NAAG_BNUM_LEVEL) {
    this.NAAG_BNUM_LEVEL = NAAG_BNUM_LEVEL;
  }
  public QueryResult with_NAAG_BNUM_LEVEL(String NAAG_BNUM_LEVEL) {
    this.NAAG_BNUM_LEVEL = NAAG_BNUM_LEVEL;
    return this;
  }
  private String NETWORK_TYPE_BNUM;
  public String get_NETWORK_TYPE_BNUM() {
    return NETWORK_TYPE_BNUM;
  }
  public void set_NETWORK_TYPE_BNUM(String NETWORK_TYPE_BNUM) {
    this.NETWORK_TYPE_BNUM = NETWORK_TYPE_BNUM;
  }
  public QueryResult with_NETWORK_TYPE_BNUM(String NETWORK_TYPE_BNUM) {
    this.NETWORK_TYPE_BNUM = NETWORK_TYPE_BNUM;
    return this;
  }
  private String RECON_NAAG_BNUM;
  public String get_RECON_NAAG_BNUM() {
    return RECON_NAAG_BNUM;
  }
  public void set_RECON_NAAG_BNUM(String RECON_NAAG_BNUM) {
    this.RECON_NAAG_BNUM = RECON_NAAG_BNUM;
  }
  public QueryResult with_RECON_NAAG_BNUM(String RECON_NAAG_BNUM) {
    this.RECON_NAAG_BNUM = RECON_NAAG_BNUM;
    return this;
  }
  private String NETWORK_ADDRESS_AGGR_BNUM;
  public String get_NETWORK_ADDRESS_AGGR_BNUM() {
    return NETWORK_ADDRESS_AGGR_BNUM;
  }
  public void set_NETWORK_ADDRESS_AGGR_BNUM(String NETWORK_ADDRESS_AGGR_BNUM) {
    this.NETWORK_ADDRESS_AGGR_BNUM = NETWORK_ADDRESS_AGGR_BNUM;
  }
  public QueryResult with_NETWORK_ADDRESS_AGGR_BNUM(String NETWORK_ADDRESS_AGGR_BNUM) {
    this.NETWORK_ADDRESS_AGGR_BNUM = NETWORK_ADDRESS_AGGR_BNUM;
    return this;
  }
  private String DERIVED_PRODUCT_INDICATOR;
  public String get_DERIVED_PRODUCT_INDICATOR() {
    return DERIVED_PRODUCT_INDICATOR;
  }
  public void set_DERIVED_PRODUCT_INDICATOR(String DERIVED_PRODUCT_INDICATOR) {
    this.DERIVED_PRODUCT_INDICATOR = DERIVED_PRODUCT_INDICATOR;
  }
  public QueryResult with_DERIVED_PRODUCT_INDICATOR(String DERIVED_PRODUCT_INDICATOR) {
    this.DERIVED_PRODUCT_INDICATOR = DERIVED_PRODUCT_INDICATOR;
    return this;
  }
  private String ANUM;
  public String get_ANUM() {
    return ANUM;
  }
  public void set_ANUM(String ANUM) {
    this.ANUM = ANUM;
  }
  public QueryResult with_ANUM(String ANUM) {
    this.ANUM = ANUM;
    return this;
  }
  private String BNUM;
  public String get_BNUM() {
    return BNUM;
  }
  public void set_BNUM(String BNUM) {
    this.BNUM = BNUM;
  }
  public QueryResult with_BNUM(String BNUM) {
    this.BNUM = BNUM;
    return this;
  }
  private String RECORD_SEQUENCE_NUMBER;
  public String get_RECORD_SEQUENCE_NUMBER() {
    return RECORD_SEQUENCE_NUMBER;
  }
  public void set_RECORD_SEQUENCE_NUMBER(String RECORD_SEQUENCE_NUMBER) {
    this.RECORD_SEQUENCE_NUMBER = RECORD_SEQUENCE_NUMBER;
  }
  public QueryResult with_RECORD_SEQUENCE_NUMBER(String RECORD_SEQUENCE_NUMBER) {
    this.RECORD_SEQUENCE_NUMBER = RECORD_SEQUENCE_NUMBER;
    return this;
  }
  private String USER_DATA;
  public String get_USER_DATA() {
    return USER_DATA;
  }
  public void set_USER_DATA(String USER_DATA) {
    this.USER_DATA = USER_DATA;
  }
  public QueryResult with_USER_DATA(String USER_DATA) {
    this.USER_DATA = USER_DATA;
    return this;
  }
  private String USER_DATA_2;
  public String get_USER_DATA_2() {
    return USER_DATA_2;
  }
  public void set_USER_DATA_2(String USER_DATA_2) {
    this.USER_DATA_2 = USER_DATA_2;
  }
  public QueryResult with_USER_DATA_2(String USER_DATA_2) {
    this.USER_DATA_2 = USER_DATA_2;
    return this;
  }
  private String USER_DATA_3;
  public String get_USER_DATA_3() {
    return USER_DATA_3;
  }
  public void set_USER_DATA_3(String USER_DATA_3) {
    this.USER_DATA_3 = USER_DATA_3;
  }
  public QueryResult with_USER_DATA_3(String USER_DATA_3) {
    this.USER_DATA_3 = USER_DATA_3;
    return this;
  }
  private java.math.BigDecimal CALL_COUNT;
  public java.math.BigDecimal get_CALL_COUNT() {
    return CALL_COUNT;
  }
  public void set_CALL_COUNT(java.math.BigDecimal CALL_COUNT) {
    this.CALL_COUNT = CALL_COUNT;
  }
  public QueryResult with_CALL_COUNT(java.math.BigDecimal CALL_COUNT) {
    this.CALL_COUNT = CALL_COUNT;
    return this;
  }
  private java.math.BigDecimal START_CALL_COUNT;
  public java.math.BigDecimal get_START_CALL_COUNT() {
    return START_CALL_COUNT;
  }
  public void set_START_CALL_COUNT(java.math.BigDecimal START_CALL_COUNT) {
    this.START_CALL_COUNT = START_CALL_COUNT;
  }
  public QueryResult with_START_CALL_COUNT(java.math.BigDecimal START_CALL_COUNT) {
    this.START_CALL_COUNT = START_CALL_COUNT;
    return this;
  }
  private java.math.BigDecimal RATE_STEP_CALL_COUNT;
  public java.math.BigDecimal get_RATE_STEP_CALL_COUNT() {
    return RATE_STEP_CALL_COUNT;
  }
  public void set_RATE_STEP_CALL_COUNT(java.math.BigDecimal RATE_STEP_CALL_COUNT) {
    this.RATE_STEP_CALL_COUNT = RATE_STEP_CALL_COUNT;
  }
  public QueryResult with_RATE_STEP_CALL_COUNT(java.math.BigDecimal RATE_STEP_CALL_COUNT) {
    this.RATE_STEP_CALL_COUNT = RATE_STEP_CALL_COUNT;
    return this;
  }
  private java.math.BigDecimal APPORTIONED_CALL_COUNT;
  public java.math.BigDecimal get_APPORTIONED_CALL_COUNT() {
    return APPORTIONED_CALL_COUNT;
  }
  public void set_APPORTIONED_CALL_COUNT(java.math.BigDecimal APPORTIONED_CALL_COUNT) {
    this.APPORTIONED_CALL_COUNT = APPORTIONED_CALL_COUNT;
  }
  public QueryResult with_APPORTIONED_CALL_COUNT(java.math.BigDecimal APPORTIONED_CALL_COUNT) {
    this.APPORTIONED_CALL_COUNT = APPORTIONED_CALL_COUNT;
    return this;
  }
  private java.math.BigDecimal APPORTIONED_DURATION_SECONDS;
  public java.math.BigDecimal get_APPORTIONED_DURATION_SECONDS() {
    return APPORTIONED_DURATION_SECONDS;
  }
  public void set_APPORTIONED_DURATION_SECONDS(java.math.BigDecimal APPORTIONED_DURATION_SECONDS) {
    this.APPORTIONED_DURATION_SECONDS = APPORTIONED_DURATION_SECONDS;
  }
  public QueryResult with_APPORTIONED_DURATION_SECONDS(java.math.BigDecimal APPORTIONED_DURATION_SECONDS) {
    this.APPORTIONED_DURATION_SECONDS = APPORTIONED_DURATION_SECONDS;
    return this;
  }
  private java.math.BigDecimal ACTUAL_USAGE;
  public java.math.BigDecimal get_ACTUAL_USAGE() {
    return ACTUAL_USAGE;
  }
  public void set_ACTUAL_USAGE(java.math.BigDecimal ACTUAL_USAGE) {
    this.ACTUAL_USAGE = ACTUAL_USAGE;
  }
  public QueryResult with_ACTUAL_USAGE(java.math.BigDecimal ACTUAL_USAGE) {
    this.ACTUAL_USAGE = ACTUAL_USAGE;
    return this;
  }
  private java.math.BigDecimal CHARGED_USAGE;
  public java.math.BigDecimal get_CHARGED_USAGE() {
    return CHARGED_USAGE;
  }
  public void set_CHARGED_USAGE(java.math.BigDecimal CHARGED_USAGE) {
    this.CHARGED_USAGE = CHARGED_USAGE;
  }
  public QueryResult with_CHARGED_USAGE(java.math.BigDecimal CHARGED_USAGE) {
    this.CHARGED_USAGE = CHARGED_USAGE;
    return this;
  }
  private String CHARGED_UNITS;
  public String get_CHARGED_UNITS() {
    return CHARGED_UNITS;
  }
  public void set_CHARGED_UNITS(String CHARGED_UNITS) {
    this.CHARGED_UNITS = CHARGED_UNITS;
  }
  public QueryResult with_CHARGED_UNITS(String CHARGED_UNITS) {
    this.CHARGED_UNITS = CHARGED_UNITS;
    return this;
  }
  private java.math.BigDecimal EVENT_DURATION;
  public java.math.BigDecimal get_EVENT_DURATION() {
    return EVENT_DURATION;
  }
  public void set_EVENT_DURATION(java.math.BigDecimal EVENT_DURATION) {
    this.EVENT_DURATION = EVENT_DURATION;
  }
  public QueryResult with_EVENT_DURATION(java.math.BigDecimal EVENT_DURATION) {
    this.EVENT_DURATION = EVENT_DURATION;
    return this;
  }
  private java.math.BigDecimal MINIMUM_CHARGE_ADJUSTMENT;
  public java.math.BigDecimal get_MINIMUM_CHARGE_ADJUSTMENT() {
    return MINIMUM_CHARGE_ADJUSTMENT;
  }
  public void set_MINIMUM_CHARGE_ADJUSTMENT(java.math.BigDecimal MINIMUM_CHARGE_ADJUSTMENT) {
    this.MINIMUM_CHARGE_ADJUSTMENT = MINIMUM_CHARGE_ADJUSTMENT;
  }
  public QueryResult with_MINIMUM_CHARGE_ADJUSTMENT(java.math.BigDecimal MINIMUM_CHARGE_ADJUSTMENT) {
    this.MINIMUM_CHARGE_ADJUSTMENT = MINIMUM_CHARGE_ADJUSTMENT;
    return this;
  }
  private java.math.BigDecimal MAXIMUM_CHARGE_ADJUSTMENT;
  public java.math.BigDecimal get_MAXIMUM_CHARGE_ADJUSTMENT() {
    return MAXIMUM_CHARGE_ADJUSTMENT;
  }
  public void set_MAXIMUM_CHARGE_ADJUSTMENT(java.math.BigDecimal MAXIMUM_CHARGE_ADJUSTMENT) {
    this.MAXIMUM_CHARGE_ADJUSTMENT = MAXIMUM_CHARGE_ADJUSTMENT;
  }
  public QueryResult with_MAXIMUM_CHARGE_ADJUSTMENT(java.math.BigDecimal MAXIMUM_CHARGE_ADJUSTMENT) {
    this.MAXIMUM_CHARGE_ADJUSTMENT = MAXIMUM_CHARGE_ADJUSTMENT;
    return this;
  }
  private java.math.BigDecimal NETWORK_DURATION;
  public java.math.BigDecimal get_NETWORK_DURATION() {
    return NETWORK_DURATION;
  }
  public void set_NETWORK_DURATION(java.math.BigDecimal NETWORK_DURATION) {
    this.NETWORK_DURATION = NETWORK_DURATION;
  }
  public QueryResult with_NETWORK_DURATION(java.math.BigDecimal NETWORK_DURATION) {
    this.NETWORK_DURATION = NETWORK_DURATION;
    return this;
  }
  private java.math.BigDecimal DATA_VOLUME;
  public java.math.BigDecimal get_DATA_VOLUME() {
    return DATA_VOLUME;
  }
  public void set_DATA_VOLUME(java.math.BigDecimal DATA_VOLUME) {
    this.DATA_VOLUME = DATA_VOLUME;
  }
  public QueryResult with_DATA_VOLUME(java.math.BigDecimal DATA_VOLUME) {
    this.DATA_VOLUME = DATA_VOLUME;
    return this;
  }
  private java.math.BigDecimal DATA_VOLUME_2;
  public java.math.BigDecimal get_DATA_VOLUME_2() {
    return DATA_VOLUME_2;
  }
  public void set_DATA_VOLUME_2(java.math.BigDecimal DATA_VOLUME_2) {
    this.DATA_VOLUME_2 = DATA_VOLUME_2;
  }
  public QueryResult with_DATA_VOLUME_2(java.math.BigDecimal DATA_VOLUME_2) {
    this.DATA_VOLUME_2 = DATA_VOLUME_2;
    return this;
  }
  private java.math.BigDecimal DATA_VOLUME_3;
  public java.math.BigDecimal get_DATA_VOLUME_3() {
    return DATA_VOLUME_3;
  }
  public void set_DATA_VOLUME_3(java.math.BigDecimal DATA_VOLUME_3) {
    this.DATA_VOLUME_3 = DATA_VOLUME_3;
  }
  public QueryResult with_DATA_VOLUME_3(java.math.BigDecimal DATA_VOLUME_3) {
    this.DATA_VOLUME_3 = DATA_VOLUME_3;
    return this;
  }
  private java.math.BigDecimal DATA_VOLUME_4;
  public java.math.BigDecimal get_DATA_VOLUME_4() {
    return DATA_VOLUME_4;
  }
  public void set_DATA_VOLUME_4(java.math.BigDecimal DATA_VOLUME_4) {
    this.DATA_VOLUME_4 = DATA_VOLUME_4;
  }
  public QueryResult with_DATA_VOLUME_4(java.math.BigDecimal DATA_VOLUME_4) {
    this.DATA_VOLUME_4 = DATA_VOLUME_4;
    return this;
  }
  private java.math.BigDecimal REVENUE_SHARE_AMOUNT_1;
  public java.math.BigDecimal get_REVENUE_SHARE_AMOUNT_1() {
    return REVENUE_SHARE_AMOUNT_1;
  }
  public void set_REVENUE_SHARE_AMOUNT_1(java.math.BigDecimal REVENUE_SHARE_AMOUNT_1) {
    this.REVENUE_SHARE_AMOUNT_1 = REVENUE_SHARE_AMOUNT_1;
  }
  public QueryResult with_REVENUE_SHARE_AMOUNT_1(java.math.BigDecimal REVENUE_SHARE_AMOUNT_1) {
    this.REVENUE_SHARE_AMOUNT_1 = REVENUE_SHARE_AMOUNT_1;
    return this;
  }
  private java.math.BigDecimal REVENUE_SHARE_AMOUNT_2;
  public java.math.BigDecimal get_REVENUE_SHARE_AMOUNT_2() {
    return REVENUE_SHARE_AMOUNT_2;
  }
  public void set_REVENUE_SHARE_AMOUNT_2(java.math.BigDecimal REVENUE_SHARE_AMOUNT_2) {
    this.REVENUE_SHARE_AMOUNT_2 = REVENUE_SHARE_AMOUNT_2;
  }
  public QueryResult with_REVENUE_SHARE_AMOUNT_2(java.math.BigDecimal REVENUE_SHARE_AMOUNT_2) {
    this.REVENUE_SHARE_AMOUNT_2 = REVENUE_SHARE_AMOUNT_2;
    return this;
  }
  private java.math.BigDecimal REVENUE_SHARE_AMOUNT_3;
  public java.math.BigDecimal get_REVENUE_SHARE_AMOUNT_3() {
    return REVENUE_SHARE_AMOUNT_3;
  }
  public void set_REVENUE_SHARE_AMOUNT_3(java.math.BigDecimal REVENUE_SHARE_AMOUNT_3) {
    this.REVENUE_SHARE_AMOUNT_3 = REVENUE_SHARE_AMOUNT_3;
  }
  public QueryResult with_REVENUE_SHARE_AMOUNT_3(java.math.BigDecimal REVENUE_SHARE_AMOUNT_3) {
    this.REVENUE_SHARE_AMOUNT_3 = REVENUE_SHARE_AMOUNT_3;
    return this;
  }
  private java.math.BigDecimal BASE_AMOUNT;
  public java.math.BigDecimal get_BASE_AMOUNT() {
    return BASE_AMOUNT;
  }
  public void set_BASE_AMOUNT(java.math.BigDecimal BASE_AMOUNT) {
    this.BASE_AMOUNT = BASE_AMOUNT;
  }
  public QueryResult with_BASE_AMOUNT(java.math.BigDecimal BASE_AMOUNT) {
    this.BASE_AMOUNT = BASE_AMOUNT;
    return this;
  }
  private java.math.BigDecimal AMOUNT;
  public java.math.BigDecimal get_AMOUNT() {
    return AMOUNT;
  }
  public void set_AMOUNT(java.math.BigDecimal AMOUNT) {
    this.AMOUNT = AMOUNT;
  }
  public QueryResult with_AMOUNT(java.math.BigDecimal AMOUNT) {
    this.AMOUNT = AMOUNT;
    return this;
  }
  private String DLYS_DETAIL_ID;
  public String get_DLYS_DETAIL_ID() {
    return DLYS_DETAIL_ID;
  }
  public void set_DLYS_DETAIL_ID(String DLYS_DETAIL_ID) {
    this.DLYS_DETAIL_ID = DLYS_DETAIL_ID;
  }
  public QueryResult with_DLYS_DETAIL_ID(String DLYS_DETAIL_ID) {
    this.DLYS_DETAIL_ID = DLYS_DETAIL_ID;
    return this;
  }
  private String TRAFFIC_PERIOD;
  public String get_TRAFFIC_PERIOD() {
    return TRAFFIC_PERIOD;
  }
  public void set_TRAFFIC_PERIOD(String TRAFFIC_PERIOD) {
    this.TRAFFIC_PERIOD = TRAFFIC_PERIOD;
  }
  public QueryResult with_TRAFFIC_PERIOD(String TRAFFIC_PERIOD) {
    this.TRAFFIC_PERIOD = TRAFFIC_PERIOD;
    return this;
  }
  private java.sql.Timestamp MESSAGE_DATE;
  public java.sql.Timestamp get_MESSAGE_DATE() {
    return MESSAGE_DATE;
  }
  public void set_MESSAGE_DATE(java.sql.Timestamp MESSAGE_DATE) {
    this.MESSAGE_DATE = MESSAGE_DATE;
  }
  public QueryResult with_MESSAGE_DATE(java.sql.Timestamp MESSAGE_DATE) {
    this.MESSAGE_DATE = MESSAGE_DATE;
    return this;
  }
  private java.sql.Timestamp BILLING_DATE;
  public java.sql.Timestamp get_BILLING_DATE() {
    return BILLING_DATE;
  }
  public void set_BILLING_DATE(java.sql.Timestamp BILLING_DATE) {
    this.BILLING_DATE = BILLING_DATE;
  }
  public QueryResult with_BILLING_DATE(java.sql.Timestamp BILLING_DATE) {
    this.BILLING_DATE = BILLING_DATE;
    return this;
  }
  private java.sql.Timestamp ADJUSTED_DATE;
  public java.sql.Timestamp get_ADJUSTED_DATE() {
    return ADJUSTED_DATE;
  }
  public void set_ADJUSTED_DATE(java.sql.Timestamp ADJUSTED_DATE) {
    this.ADJUSTED_DATE = ADJUSTED_DATE;
  }
  public QueryResult with_ADJUSTED_DATE(java.sql.Timestamp ADJUSTED_DATE) {
    this.ADJUSTED_DATE = ADJUSTED_DATE;
    return this;
  }
  private java.sql.Timestamp PROCESS_DATE;
  public java.sql.Timestamp get_PROCESS_DATE() {
    return PROCESS_DATE;
  }
  public void set_PROCESS_DATE(java.sql.Timestamp PROCESS_DATE) {
    this.PROCESS_DATE = PROCESS_DATE;
  }
  public QueryResult with_PROCESS_DATE(java.sql.Timestamp PROCESS_DATE) {
    this.PROCESS_DATE = PROCESS_DATE;
    return this;
  }
  private java.sql.Timestamp EVENT_START_DATE;
  public java.sql.Timestamp get_EVENT_START_DATE() {
    return EVENT_START_DATE;
  }
  public void set_EVENT_START_DATE(java.sql.Timestamp EVENT_START_DATE) {
    this.EVENT_START_DATE = EVENT_START_DATE;
  }
  public QueryResult with_EVENT_START_DATE(java.sql.Timestamp EVENT_START_DATE) {
    this.EVENT_START_DATE = EVENT_START_DATE;
    return this;
  }
  private String EVENT_START_TIME;
  public String get_EVENT_START_TIME() {
    return EVENT_START_TIME;
  }
  public void set_EVENT_START_TIME(String EVENT_START_TIME) {
    this.EVENT_START_TIME = EVENT_START_TIME;
  }
  public QueryResult with_EVENT_START_TIME(String EVENT_START_TIME) {
    this.EVENT_START_TIME = EVENT_START_TIME;
    return this;
  }
  private java.math.BigDecimal NETWORK_START_DATE;
  public java.math.BigDecimal get_NETWORK_START_DATE() {
    return NETWORK_START_DATE;
  }
  public void set_NETWORK_START_DATE(java.math.BigDecimal NETWORK_START_DATE) {
    this.NETWORK_START_DATE = NETWORK_START_DATE;
  }
  public QueryResult with_NETWORK_START_DATE(java.math.BigDecimal NETWORK_START_DATE) {
    this.NETWORK_START_DATE = NETWORK_START_DATE;
    return this;
  }
  private java.math.BigDecimal NETWORK_START_TIME;
  public java.math.BigDecimal get_NETWORK_START_TIME() {
    return NETWORK_START_TIME;
  }
  public void set_NETWORK_START_TIME(java.math.BigDecimal NETWORK_START_TIME) {
    this.NETWORK_START_TIME = NETWORK_START_TIME;
  }
  public QueryResult with_NETWORK_START_TIME(java.math.BigDecimal NETWORK_START_TIME) {
    this.NETWORK_START_TIME = NETWORK_START_TIME;
    return this;
  }
  private java.math.BigDecimal BILLING_START_TIME;
  public java.math.BigDecimal get_BILLING_START_TIME() {
    return BILLING_START_TIME;
  }
  public void set_BILLING_START_TIME(java.math.BigDecimal BILLING_START_TIME) {
    this.BILLING_START_TIME = BILLING_START_TIME;
  }
  public QueryResult with_BILLING_START_TIME(java.math.BigDecimal BILLING_START_TIME) {
    this.BILLING_START_TIME = BILLING_START_TIME;
    return this;
  }
  private java.math.BigDecimal BILLING_END_TIME;
  public java.math.BigDecimal get_BILLING_END_TIME() {
    return BILLING_END_TIME;
  }
  public void set_BILLING_END_TIME(java.math.BigDecimal BILLING_END_TIME) {
    this.BILLING_END_TIME = BILLING_END_TIME;
  }
  public QueryResult with_BILLING_END_TIME(java.math.BigDecimal BILLING_END_TIME) {
    this.BILLING_END_TIME = BILLING_END_TIME;
    return this;
  }
  private String FLAT_RATE_CHARGE;
  public String get_FLAT_RATE_CHARGE() {
    return FLAT_RATE_CHARGE;
  }
  public void set_FLAT_RATE_CHARGE(String FLAT_RATE_CHARGE) {
    this.FLAT_RATE_CHARGE = FLAT_RATE_CHARGE;
  }
  public QueryResult with_FLAT_RATE_CHARGE(String FLAT_RATE_CHARGE) {
    this.FLAT_RATE_CHARGE = FLAT_RATE_CHARGE;
    return this;
  }
  private java.math.BigDecimal RATE_STEP_FLAT_CHARGE;
  public java.math.BigDecimal get_RATE_STEP_FLAT_CHARGE() {
    return RATE_STEP_FLAT_CHARGE;
  }
  public void set_RATE_STEP_FLAT_CHARGE(java.math.BigDecimal RATE_STEP_FLAT_CHARGE) {
    this.RATE_STEP_FLAT_CHARGE = RATE_STEP_FLAT_CHARGE;
  }
  public QueryResult with_RATE_STEP_FLAT_CHARGE(java.math.BigDecimal RATE_STEP_FLAT_CHARGE) {
    this.RATE_STEP_FLAT_CHARGE = RATE_STEP_FLAT_CHARGE;
    return this;
  }
  private java.math.BigDecimal UNIT_COST_USED;
  public java.math.BigDecimal get_UNIT_COST_USED() {
    return UNIT_COST_USED;
  }
  public void set_UNIT_COST_USED(java.math.BigDecimal UNIT_COST_USED) {
    this.UNIT_COST_USED = UNIT_COST_USED;
  }
  public QueryResult with_UNIT_COST_USED(java.math.BigDecimal UNIT_COST_USED) {
    this.UNIT_COST_USED = UNIT_COST_USED;
    return this;
  }
  private java.math.BigDecimal CHARGE_BUNDLE;
  public java.math.BigDecimal get_CHARGE_BUNDLE() {
    return CHARGE_BUNDLE;
  }
  public void set_CHARGE_BUNDLE(java.math.BigDecimal CHARGE_BUNDLE) {
    this.CHARGE_BUNDLE = CHARGE_BUNDLE;
  }
  public QueryResult with_CHARGE_BUNDLE(java.math.BigDecimal CHARGE_BUNDLE) {
    this.CHARGE_BUNDLE = CHARGE_BUNDLE;
    return this;
  }
  private java.math.BigDecimal BASE_AMOUNT_FACTOR_USED;
  public java.math.BigDecimal get_BASE_AMOUNT_FACTOR_USED() {
    return BASE_AMOUNT_FACTOR_USED;
  }
  public void set_BASE_AMOUNT_FACTOR_USED(java.math.BigDecimal BASE_AMOUNT_FACTOR_USED) {
    this.BASE_AMOUNT_FACTOR_USED = BASE_AMOUNT_FACTOR_USED;
  }
  public QueryResult with_BASE_AMOUNT_FACTOR_USED(java.math.BigDecimal BASE_AMOUNT_FACTOR_USED) {
    this.BASE_AMOUNT_FACTOR_USED = BASE_AMOUNT_FACTOR_USED;
    return this;
  }
  private java.math.BigDecimal REFUND_FACTOR;
  public java.math.BigDecimal get_REFUND_FACTOR() {
    return REFUND_FACTOR;
  }
  public void set_REFUND_FACTOR(java.math.BigDecimal REFUND_FACTOR) {
    this.REFUND_FACTOR = REFUND_FACTOR;
  }
  public QueryResult with_REFUND_FACTOR(java.math.BigDecimal REFUND_FACTOR) {
    this.REFUND_FACTOR = REFUND_FACTOR;
    return this;
  }
  private String CURRENCY;
  public String get_CURRENCY() {
    return CURRENCY;
  }
  public void set_CURRENCY(String CURRENCY) {
    this.CURRENCY = CURRENCY;
  }
  public QueryResult with_CURRENCY(String CURRENCY) {
    this.CURRENCY = CURRENCY;
    return this;
  }
  private java.math.BigDecimal CURRENCY_CONVERSION_RATE;
  public java.math.BigDecimal get_CURRENCY_CONVERSION_RATE() {
    return CURRENCY_CONVERSION_RATE;
  }
  public void set_CURRENCY_CONVERSION_RATE(java.math.BigDecimal CURRENCY_CONVERSION_RATE) {
    this.CURRENCY_CONVERSION_RATE = CURRENCY_CONVERSION_RATE;
  }
  public QueryResult with_CURRENCY_CONVERSION_RATE(java.math.BigDecimal CURRENCY_CONVERSION_RATE) {
    this.CURRENCY_CONVERSION_RATE = CURRENCY_CONVERSION_RATE;
    return this;
  }
  private String BASE_UNIT;
  public String get_BASE_UNIT() {
    return BASE_UNIT;
  }
  public void set_BASE_UNIT(String BASE_UNIT) {
    this.BASE_UNIT = BASE_UNIT;
  }
  public QueryResult with_BASE_UNIT(String BASE_UNIT) {
    this.BASE_UNIT = BASE_UNIT;
    return this;
  }
  private String RATE_UNIT;
  public String get_RATE_UNIT() {
    return RATE_UNIT;
  }
  public void set_RATE_UNIT(String RATE_UNIT) {
    this.RATE_UNIT = RATE_UNIT;
  }
  public QueryResult with_RATE_UNIT(String RATE_UNIT) {
    this.RATE_UNIT = RATE_UNIT;
    return this;
  }
  private java.math.BigDecimal ROUNDED_UNIT_ID;
  public java.math.BigDecimal get_ROUNDED_UNIT_ID() {
    return ROUNDED_UNIT_ID;
  }
  public void set_ROUNDED_UNIT_ID(java.math.BigDecimal ROUNDED_UNIT_ID) {
    this.ROUNDED_UNIT_ID = ROUNDED_UNIT_ID;
  }
  public QueryResult with_ROUNDED_UNIT_ID(java.math.BigDecimal ROUNDED_UNIT_ID) {
    this.ROUNDED_UNIT_ID = ROUNDED_UNIT_ID;
    return this;
  }
  private java.math.BigDecimal RECORD_TYPE;
  public java.math.BigDecimal get_RECORD_TYPE() {
    return RECORD_TYPE;
  }
  public void set_RECORD_TYPE(java.math.BigDecimal RECORD_TYPE) {
    this.RECORD_TYPE = RECORD_TYPE;
  }
  public QueryResult with_RECORD_TYPE(java.math.BigDecimal RECORD_TYPE) {
    this.RECORD_TYPE = RECORD_TYPE;
    return this;
  }
  private java.math.BigDecimal LINK_FIELD;
  public java.math.BigDecimal get_LINK_FIELD() {
    return LINK_FIELD;
  }
  public void set_LINK_FIELD(java.math.BigDecimal LINK_FIELD) {
    this.LINK_FIELD = LINK_FIELD;
  }
  public QueryResult with_LINK_FIELD(java.math.BigDecimal LINK_FIELD) {
    this.LINK_FIELD = LINK_FIELD;
    return this;
  }
  private java.math.BigDecimal REASON_FOR_CLEARDOWN;
  public java.math.BigDecimal get_REASON_FOR_CLEARDOWN() {
    return REASON_FOR_CLEARDOWN;
  }
  public void set_REASON_FOR_CLEARDOWN(java.math.BigDecimal REASON_FOR_CLEARDOWN) {
    this.REASON_FOR_CLEARDOWN = REASON_FOR_CLEARDOWN;
  }
  public QueryResult with_REASON_FOR_CLEARDOWN(java.math.BigDecimal REASON_FOR_CLEARDOWN) {
    this.REASON_FOR_CLEARDOWN = REASON_FOR_CLEARDOWN;
    return this;
  }
  private java.math.BigDecimal REPAIR_INDICATOR;
  public java.math.BigDecimal get_REPAIR_INDICATOR() {
    return REPAIR_INDICATOR;
  }
  public void set_REPAIR_INDICATOR(java.math.BigDecimal REPAIR_INDICATOR) {
    this.REPAIR_INDICATOR = REPAIR_INDICATOR;
  }
  public QueryResult with_REPAIR_INDICATOR(java.math.BigDecimal REPAIR_INDICATOR) {
    this.REPAIR_INDICATOR = REPAIR_INDICATOR;
    return this;
  }
  private java.math.BigDecimal RATED_BILLING_PERIOD;
  public java.math.BigDecimal get_RATED_BILLING_PERIOD() {
    return RATED_BILLING_PERIOD;
  }
  public void set_RATED_BILLING_PERIOD(java.math.BigDecimal RATED_BILLING_PERIOD) {
    this.RATED_BILLING_PERIOD = RATED_BILLING_PERIOD;
  }
  public QueryResult with_RATED_BILLING_PERIOD(java.math.BigDecimal RATED_BILLING_PERIOD) {
    this.RATED_BILLING_PERIOD = RATED_BILLING_PERIOD;
    return this;
  }
  private java.math.BigDecimal TRAFFIC_MOVEMENT_CTR;
  public java.math.BigDecimal get_TRAFFIC_MOVEMENT_CTR() {
    return TRAFFIC_MOVEMENT_CTR;
  }
  public void set_TRAFFIC_MOVEMENT_CTR(java.math.BigDecimal TRAFFIC_MOVEMENT_CTR) {
    this.TRAFFIC_MOVEMENT_CTR = TRAFFIC_MOVEMENT_CTR;
  }
  public QueryResult with_TRAFFIC_MOVEMENT_CTR(java.math.BigDecimal TRAFFIC_MOVEMENT_CTR) {
    this.TRAFFIC_MOVEMENT_CTR = TRAFFIC_MOVEMENT_CTR;
    return this;
  }
  private String RTE_LOOKUP_STYLE;
  public String get_RTE_LOOKUP_STYLE() {
    return RTE_LOOKUP_STYLE;
  }
  public void set_RTE_LOOKUP_STYLE(String RTE_LOOKUP_STYLE) {
    this.RTE_LOOKUP_STYLE = RTE_LOOKUP_STYLE;
  }
  public QueryResult with_RTE_LOOKUP_STYLE(String RTE_LOOKUP_STYLE) {
    this.RTE_LOOKUP_STYLE = RTE_LOOKUP_STYLE;
    return this;
  }
  private String CDR_SOURCE;
  public String get_CDR_SOURCE() {
    return CDR_SOURCE;
  }
  public void set_CDR_SOURCE(String CDR_SOURCE) {
    this.CDR_SOURCE = CDR_SOURCE;
  }
  public QueryResult with_CDR_SOURCE(String CDR_SOURCE) {
    this.CDR_SOURCE = CDR_SOURCE;
    return this;
  }
  private String CARRIER_DESTINATION_NAME;
  public String get_CARRIER_DESTINATION_NAME() {
    return CARRIER_DESTINATION_NAME;
  }
  public void set_CARRIER_DESTINATION_NAME(String CARRIER_DESTINATION_NAME) {
    this.CARRIER_DESTINATION_NAME = CARRIER_DESTINATION_NAME;
  }
  public QueryResult with_CARRIER_DESTINATION_NAME(String CARRIER_DESTINATION_NAME) {
    this.CARRIER_DESTINATION_NAME = CARRIER_DESTINATION_NAME;
    return this;
  }
  private String CARRIER_DESTINATION_ALIAS;
  public String get_CARRIER_DESTINATION_ALIAS() {
    return CARRIER_DESTINATION_ALIAS;
  }
  public void set_CARRIER_DESTINATION_ALIAS(String CARRIER_DESTINATION_ALIAS) {
    this.CARRIER_DESTINATION_ALIAS = CARRIER_DESTINATION_ALIAS;
  }
  public QueryResult with_CARRIER_DESTINATION_ALIAS(String CARRIER_DESTINATION_ALIAS) {
    this.CARRIER_DESTINATION_ALIAS = CARRIER_DESTINATION_ALIAS;
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
    equal = equal && (this.CDR_FILE_NO == null ? that.CDR_FILE_NO == null : this.CDR_FILE_NO.equals(that.CDR_FILE_NO));
    equal = equal && (this.CDR_UCI_NO == null ? that.CDR_UCI_NO == null : this.CDR_UCI_NO.equals(that.CDR_UCI_NO));
    equal = equal && (this.CDR_UCI_ELEMENT == null ? that.CDR_UCI_ELEMENT == null : this.CDR_UCI_ELEMENT.equals(that.CDR_UCI_ELEMENT));
    equal = equal && (this.CDR_KEY_ID == null ? that.CDR_KEY_ID == null : this.CDR_KEY_ID.equals(that.CDR_KEY_ID));
    equal = equal && (this.CALL_SCENARIO == null ? that.CALL_SCENARIO == null : this.CALL_SCENARIO.equals(that.CALL_SCENARIO));
    equal = equal && (this.REMUNERATION_INDICATOR == null ? that.REMUNERATION_INDICATOR == null : this.REMUNERATION_INDICATOR.equals(that.REMUNERATION_INDICATOR));
    equal = equal && (this.BILLING_BASE_DIRECTION == null ? that.BILLING_BASE_DIRECTION == null : this.BILLING_BASE_DIRECTION.equals(that.BILLING_BASE_DIRECTION));
    equal = equal && (this.FRANCHISE == null ? that.FRANCHISE == null : this.FRANCHISE.equals(that.FRANCHISE));
    equal = equal && (this.PRODUCT_GROUP == null ? that.PRODUCT_GROUP == null : this.PRODUCT_GROUP.equals(that.PRODUCT_GROUP));
    equal = equal && (this.REVERSE_INDICATOR == null ? that.REVERSE_INDICATOR == null : this.REVERSE_INDICATOR.equals(that.REVERSE_INDICATOR));
    equal = equal && (this.EVENT_DIRECTION == null ? that.EVENT_DIRECTION == null : this.EVENT_DIRECTION.equals(that.EVENT_DIRECTION));
    equal = equal && (this.DERIVED_EVENT_DIRECTION == null ? that.DERIVED_EVENT_DIRECTION == null : this.DERIVED_EVENT_DIRECTION.equals(that.DERIVED_EVENT_DIRECTION));
    equal = equal && (this.BILLING_RATING_SCENARIO_TYPE == null ? that.BILLING_RATING_SCENARIO_TYPE == null : this.BILLING_RATING_SCENARIO_TYPE.equals(that.BILLING_RATING_SCENARIO_TYPE));
    equal = equal && (this.BILLED_PRODUCT == null ? that.BILLED_PRODUCT == null : this.BILLED_PRODUCT.equals(that.BILLED_PRODUCT));
    equal = equal && (this.RATING_SCENARIO == null ? that.RATING_SCENARIO == null : this.RATING_SCENARIO.equals(that.RATING_SCENARIO));
    equal = equal && (this.RATING_RULE_DEPENDENCY_IND == null ? that.RATING_RULE_DEPENDENCY_IND == null : this.RATING_RULE_DEPENDENCY_IND.equals(that.RATING_RULE_DEPENDENCY_IND));
    equal = equal && (this.ESTIMATE_INDICATOR == null ? that.ESTIMATE_INDICATOR == null : this.ESTIMATE_INDICATOR.equals(that.ESTIMATE_INDICATOR));
    equal = equal && (this.ACCOUNTING_METHOD == null ? that.ACCOUNTING_METHOD == null : this.ACCOUNTING_METHOD.equals(that.ACCOUNTING_METHOD));
    equal = equal && (this.CASH_FLOW == null ? that.CASH_FLOW == null : this.CASH_FLOW.equals(that.CASH_FLOW));
    equal = equal && (this.COMPONENT_DIRECTION == null ? that.COMPONENT_DIRECTION == null : this.COMPONENT_DIRECTION.equals(that.COMPONENT_DIRECTION));
    equal = equal && (this.STATEMENT_DIRECTION == null ? that.STATEMENT_DIRECTION == null : this.STATEMENT_DIRECTION.equals(that.STATEMENT_DIRECTION));
    equal = equal && (this.BILLING_METHOD == null ? that.BILLING_METHOD == null : this.BILLING_METHOD.equals(that.BILLING_METHOD));
    equal = equal && (this.RATING_COMPONENT == null ? that.RATING_COMPONENT == null : this.RATING_COMPONENT.equals(that.RATING_COMPONENT));
    equal = equal && (this.CALL_ATTEMPT_INDICATOR == null ? that.CALL_ATTEMPT_INDICATOR == null : this.CALL_ATTEMPT_INDICATOR.equals(that.CALL_ATTEMPT_INDICATOR));
    equal = equal && (this.PARAMETER_MATRIX == null ? that.PARAMETER_MATRIX == null : this.PARAMETER_MATRIX.equals(that.PARAMETER_MATRIX));
    equal = equal && (this.PARAMETER_MATRIX_LINE == null ? that.PARAMETER_MATRIX_LINE == null : this.PARAMETER_MATRIX_LINE.equals(that.PARAMETER_MATRIX_LINE));
    equal = equal && (this.STEP_NUMBER == null ? that.STEP_NUMBER == null : this.STEP_NUMBER.equals(that.STEP_NUMBER));
    equal = equal && (this.RATE_TYPE == null ? that.RATE_TYPE == null : this.RATE_TYPE.equals(that.RATE_TYPE));
    equal = equal && (this.TIME_PREMIUM == null ? that.TIME_PREMIUM == null : this.TIME_PREMIUM.equals(that.TIME_PREMIUM));
    equal = equal && (this.BILLING_OPERATOR == null ? that.BILLING_OPERATOR == null : this.BILLING_OPERATOR.equals(that.BILLING_OPERATOR));
    equal = equal && (this.RATE_OWNER_OPERATOR == null ? that.RATE_OWNER_OPERATOR == null : this.RATE_OWNER_OPERATOR.equals(that.RATE_OWNER_OPERATOR));
    equal = equal && (this.SETTLEMENT_OPERATOR == null ? that.SETTLEMENT_OPERATOR == null : this.SETTLEMENT_OPERATOR.equals(that.SETTLEMENT_OPERATOR));
    equal = equal && (this.RATE_NAME == null ? that.RATE_NAME == null : this.RATE_NAME.equals(that.RATE_NAME));
    equal = equal && (this.TIER == null ? that.TIER == null : this.TIER.equals(that.TIER));
    equal = equal && (this.TIER_GROUP == null ? that.TIER_GROUP == null : this.TIER_GROUP.equals(that.TIER_GROUP));
    equal = equal && (this.TIER_TYPE == null ? that.TIER_TYPE == null : this.TIER_TYPE.equals(that.TIER_TYPE));
    equal = equal && (this.OPTIMUM_POI == null ? that.OPTIMUM_POI == null : this.OPTIMUM_POI.equals(that.OPTIMUM_POI));
    equal = equal && (this.ADHOC_SUMMARY_IND == null ? that.ADHOC_SUMMARY_IND == null : this.ADHOC_SUMMARY_IND.equals(that.ADHOC_SUMMARY_IND));
    equal = equal && (this.BILLING_SUMMARY_IND == null ? that.BILLING_SUMMARY_IND == null : this.BILLING_SUMMARY_IND.equals(that.BILLING_SUMMARY_IND));
    equal = equal && (this.COMPLEMENTARY_SUMMARY_IND == null ? that.COMPLEMENTARY_SUMMARY_IND == null : this.COMPLEMENTARY_SUMMARY_IND.equals(that.COMPLEMENTARY_SUMMARY_IND));
    equal = equal && (this.TRAFFIC_SUMMARY_IND == null ? that.TRAFFIC_SUMMARY_IND == null : this.TRAFFIC_SUMMARY_IND.equals(that.TRAFFIC_SUMMARY_IND));
    equal = equal && (this.MINIMUM_CHARGE_IND == null ? that.MINIMUM_CHARGE_IND == null : this.MINIMUM_CHARGE_IND.equals(that.MINIMUM_CHARGE_IND));
    equal = equal && (this.CALL_DURATION_BUCKET == null ? that.CALL_DURATION_BUCKET == null : this.CALL_DURATION_BUCKET.equals(that.CALL_DURATION_BUCKET));
    equal = equal && (this.TIME_OF_DAY_BUCKET == null ? that.TIME_OF_DAY_BUCKET == null : this.TIME_OF_DAY_BUCKET.equals(that.TIME_OF_DAY_BUCKET));
    equal = equal && (this.USER_SUMMARISATION == null ? that.USER_SUMMARISATION == null : this.USER_SUMMARISATION.equals(that.USER_SUMMARISATION));
    equal = equal && (this.INCOMING_NODE == null ? that.INCOMING_NODE == null : this.INCOMING_NODE.equals(that.INCOMING_NODE));
    equal = equal && (this.INCOMING_PRODUCT == null ? that.INCOMING_PRODUCT == null : this.INCOMING_PRODUCT.equals(that.INCOMING_PRODUCT));
    equal = equal && (this.INCOMING_OPERATOR == null ? that.INCOMING_OPERATOR == null : this.INCOMING_OPERATOR.equals(that.INCOMING_OPERATOR));
    equal = equal && (this.INCOMING_PATH == null ? that.INCOMING_PATH == null : this.INCOMING_PATH.equals(that.INCOMING_PATH));
    equal = equal && (this.INCOMING_POI == null ? that.INCOMING_POI == null : this.INCOMING_POI.equals(that.INCOMING_POI));
    equal = equal && (this.OUTGOING_NODE == null ? that.OUTGOING_NODE == null : this.OUTGOING_NODE.equals(that.OUTGOING_NODE));
    equal = equal && (this.OUTGOING_PRODUCT == null ? that.OUTGOING_PRODUCT == null : this.OUTGOING_PRODUCT.equals(that.OUTGOING_PRODUCT));
    equal = equal && (this.OUTGOING_OPERATOR == null ? that.OUTGOING_OPERATOR == null : this.OUTGOING_OPERATOR.equals(that.OUTGOING_OPERATOR));
    equal = equal && (this.OUTGOING_PATH == null ? that.OUTGOING_PATH == null : this.OUTGOING_PATH.equals(that.OUTGOING_PATH));
    equal = equal && (this.OUTGOING_POI == null ? that.OUTGOING_POI == null : this.OUTGOING_POI.equals(that.OUTGOING_POI));
    equal = equal && (this.TRAFFIC_RATING_SCENARIO_TYPE == null ? that.TRAFFIC_RATING_SCENARIO_TYPE == null : this.TRAFFIC_RATING_SCENARIO_TYPE.equals(that.TRAFFIC_RATING_SCENARIO_TYPE));
    equal = equal && (this.TRAFFIC_ROUTE_TYPE == null ? that.TRAFFIC_ROUTE_TYPE == null : this.TRAFFIC_ROUTE_TYPE.equals(that.TRAFFIC_ROUTE_TYPE));
    equal = equal && (this.TRAFFIC_ROUTE == null ? that.TRAFFIC_ROUTE == null : this.TRAFFIC_ROUTE.equals(that.TRAFFIC_ROUTE));
    equal = equal && (this.TRAFFIC_AGREEMENT_OPERATOR == null ? that.TRAFFIC_AGREEMENT_OPERATOR == null : this.TRAFFIC_AGREEMENT_OPERATOR.equals(that.TRAFFIC_AGREEMENT_OPERATOR));
    equal = equal && (this.TRAFFIC_NEGOTIATION_DIR == null ? that.TRAFFIC_NEGOTIATION_DIR == null : this.TRAFFIC_NEGOTIATION_DIR.equals(that.TRAFFIC_NEGOTIATION_DIR));
    equal = equal && (this.BILLING_ROUTE_TYPE == null ? that.BILLING_ROUTE_TYPE == null : this.BILLING_ROUTE_TYPE.equals(that.BILLING_ROUTE_TYPE));
    equal = equal && (this.BILLING_ROUTE == null ? that.BILLING_ROUTE == null : this.BILLING_ROUTE.equals(that.BILLING_ROUTE));
    equal = equal && (this.BILLING_AGREEMENT_OPERATOR == null ? that.BILLING_AGREEMENT_OPERATOR == null : this.BILLING_AGREEMENT_OPERATOR.equals(that.BILLING_AGREEMENT_OPERATOR));
    equal = equal && (this.BILLING_NEGOTIATION_DIR == null ? that.BILLING_NEGOTIATION_DIR == null : this.BILLING_NEGOTIATION_DIR.equals(that.BILLING_NEGOTIATION_DIR));
    equal = equal && (this.ROUTE_IDENTIFIER == null ? that.ROUTE_IDENTIFIER == null : this.ROUTE_IDENTIFIER.equals(that.ROUTE_IDENTIFIER));
    equal = equal && (this.REFILE_INDICATOR == null ? that.REFILE_INDICATOR == null : this.REFILE_INDICATOR.equals(that.REFILE_INDICATOR));
    equal = equal && (this.AGREEMENT_NAAG_ANUM_LEVEL == null ? that.AGREEMENT_NAAG_ANUM_LEVEL == null : this.AGREEMENT_NAAG_ANUM_LEVEL.equals(that.AGREEMENT_NAAG_ANUM_LEVEL));
    equal = equal && (this.AGREEMENT_NAAG_ANUM == null ? that.AGREEMENT_NAAG_ANUM == null : this.AGREEMENT_NAAG_ANUM.equals(that.AGREEMENT_NAAG_ANUM));
    equal = equal && (this.AGREEMENT_NAAG_BNUM_LEVEL == null ? that.AGREEMENT_NAAG_BNUM_LEVEL == null : this.AGREEMENT_NAAG_BNUM_LEVEL.equals(that.AGREEMENT_NAAG_BNUM_LEVEL));
    equal = equal && (this.AGREEMENT_NAAG_BNUM == null ? that.AGREEMENT_NAAG_BNUM == null : this.AGREEMENT_NAAG_BNUM.equals(that.AGREEMENT_NAAG_BNUM));
    equal = equal && (this.WORLD_VIEW_NAAG_ANUM == null ? that.WORLD_VIEW_NAAG_ANUM == null : this.WORLD_VIEW_NAAG_ANUM.equals(that.WORLD_VIEW_NAAG_ANUM));
    equal = equal && (this.WORLD_VIEW_ANUM == null ? that.WORLD_VIEW_ANUM == null : this.WORLD_VIEW_ANUM.equals(that.WORLD_VIEW_ANUM));
    equal = equal && (this.WORLD_VIEW_NAAG_BNUM == null ? that.WORLD_VIEW_NAAG_BNUM == null : this.WORLD_VIEW_NAAG_BNUM.equals(that.WORLD_VIEW_NAAG_BNUM));
    equal = equal && (this.WORLD_VIEW_BNUM == null ? that.WORLD_VIEW_BNUM == null : this.WORLD_VIEW_BNUM.equals(that.WORLD_VIEW_BNUM));
    equal = equal && (this.TRAFFIC_BASE_DIRECTION == null ? that.TRAFFIC_BASE_DIRECTION == null : this.TRAFFIC_BASE_DIRECTION.equals(that.TRAFFIC_BASE_DIRECTION));
    equal = equal && (this.DATA_UNIT == null ? that.DATA_UNIT == null : this.DATA_UNIT.equals(that.DATA_UNIT));
    equal = equal && (this.DATA_UNIT_2 == null ? that.DATA_UNIT_2 == null : this.DATA_UNIT_2.equals(that.DATA_UNIT_2));
    equal = equal && (this.DATA_UNIT_3 == null ? that.DATA_UNIT_3 == null : this.DATA_UNIT_3.equals(that.DATA_UNIT_3));
    equal = equal && (this.DATA_UNIT_4 == null ? that.DATA_UNIT_4 == null : this.DATA_UNIT_4.equals(that.DATA_UNIT_4));
    equal = equal && (this.DISCRETE_RATING_PARAMETER_1 == null ? that.DISCRETE_RATING_PARAMETER_1 == null : this.DISCRETE_RATING_PARAMETER_1.equals(that.DISCRETE_RATING_PARAMETER_1));
    equal = equal && (this.DISCRETE_RATING_PARAMETER_2 == null ? that.DISCRETE_RATING_PARAMETER_2 == null : this.DISCRETE_RATING_PARAMETER_2.equals(that.DISCRETE_RATING_PARAMETER_2));
    equal = equal && (this.DISCRETE_RATING_PARAMETER_3 == null ? that.DISCRETE_RATING_PARAMETER_3 == null : this.DISCRETE_RATING_PARAMETER_3.equals(that.DISCRETE_RATING_PARAMETER_3));
    equal = equal && (this.REVENUE_SHARE_CURRENCY_1 == null ? that.REVENUE_SHARE_CURRENCY_1 == null : this.REVENUE_SHARE_CURRENCY_1.equals(that.REVENUE_SHARE_CURRENCY_1));
    equal = equal && (this.REVENUE_SHARE_CURRENCY_2 == null ? that.REVENUE_SHARE_CURRENCY_2 == null : this.REVENUE_SHARE_CURRENCY_2.equals(that.REVENUE_SHARE_CURRENCY_2));
    equal = equal && (this.REVENUE_SHARE_CURRENCY_3 == null ? that.REVENUE_SHARE_CURRENCY_3 == null : this.REVENUE_SHARE_CURRENCY_3.equals(that.REVENUE_SHARE_CURRENCY_3));
    equal = equal && (this.ANUM_OPERATOR == null ? that.ANUM_OPERATOR == null : this.ANUM_OPERATOR.equals(that.ANUM_OPERATOR));
    equal = equal && (this.ANUM_CNP == null ? that.ANUM_CNP == null : this.ANUM_CNP.equals(that.ANUM_CNP));
    equal = equal && (this.TRAFFIC_NAAG == null ? that.TRAFFIC_NAAG == null : this.TRAFFIC_NAAG.equals(that.TRAFFIC_NAAG));
    equal = equal && (this.NAAG_ANUM_LEVEL == null ? that.NAAG_ANUM_LEVEL == null : this.NAAG_ANUM_LEVEL.equals(that.NAAG_ANUM_LEVEL));
    equal = equal && (this.RECON_NAAG_ANUM == null ? that.RECON_NAAG_ANUM == null : this.RECON_NAAG_ANUM.equals(that.RECON_NAAG_ANUM));
    equal = equal && (this.NETWORK_ADDRESS_AGGR_ANUM == null ? that.NETWORK_ADDRESS_AGGR_ANUM == null : this.NETWORK_ADDRESS_AGGR_ANUM.equals(that.NETWORK_ADDRESS_AGGR_ANUM));
    equal = equal && (this.NETWORK_TYPE_ANUM == null ? that.NETWORK_TYPE_ANUM == null : this.NETWORK_TYPE_ANUM.equals(that.NETWORK_TYPE_ANUM));
    equal = equal && (this.BNUM_OPERATOR == null ? that.BNUM_OPERATOR == null : this.BNUM_OPERATOR.equals(that.BNUM_OPERATOR));
    equal = equal && (this.BNUM_CNP == null ? that.BNUM_CNP == null : this.BNUM_CNP.equals(that.BNUM_CNP));
    equal = equal && (this.NAAG_BNUM_LEVEL == null ? that.NAAG_BNUM_LEVEL == null : this.NAAG_BNUM_LEVEL.equals(that.NAAG_BNUM_LEVEL));
    equal = equal && (this.NETWORK_TYPE_BNUM == null ? that.NETWORK_TYPE_BNUM == null : this.NETWORK_TYPE_BNUM.equals(that.NETWORK_TYPE_BNUM));
    equal = equal && (this.RECON_NAAG_BNUM == null ? that.RECON_NAAG_BNUM == null : this.RECON_NAAG_BNUM.equals(that.RECON_NAAG_BNUM));
    equal = equal && (this.NETWORK_ADDRESS_AGGR_BNUM == null ? that.NETWORK_ADDRESS_AGGR_BNUM == null : this.NETWORK_ADDRESS_AGGR_BNUM.equals(that.NETWORK_ADDRESS_AGGR_BNUM));
    equal = equal && (this.DERIVED_PRODUCT_INDICATOR == null ? that.DERIVED_PRODUCT_INDICATOR == null : this.DERIVED_PRODUCT_INDICATOR.equals(that.DERIVED_PRODUCT_INDICATOR));
    equal = equal && (this.ANUM == null ? that.ANUM == null : this.ANUM.equals(that.ANUM));
    equal = equal && (this.BNUM == null ? that.BNUM == null : this.BNUM.equals(that.BNUM));
    equal = equal && (this.RECORD_SEQUENCE_NUMBER == null ? that.RECORD_SEQUENCE_NUMBER == null : this.RECORD_SEQUENCE_NUMBER.equals(that.RECORD_SEQUENCE_NUMBER));
    equal = equal && (this.USER_DATA == null ? that.USER_DATA == null : this.USER_DATA.equals(that.USER_DATA));
    equal = equal && (this.USER_DATA_2 == null ? that.USER_DATA_2 == null : this.USER_DATA_2.equals(that.USER_DATA_2));
    equal = equal && (this.USER_DATA_3 == null ? that.USER_DATA_3 == null : this.USER_DATA_3.equals(that.USER_DATA_3));
    equal = equal && (this.CALL_COUNT == null ? that.CALL_COUNT == null : this.CALL_COUNT.equals(that.CALL_COUNT));
    equal = equal && (this.START_CALL_COUNT == null ? that.START_CALL_COUNT == null : this.START_CALL_COUNT.equals(that.START_CALL_COUNT));
    equal = equal && (this.RATE_STEP_CALL_COUNT == null ? that.RATE_STEP_CALL_COUNT == null : this.RATE_STEP_CALL_COUNT.equals(that.RATE_STEP_CALL_COUNT));
    equal = equal && (this.APPORTIONED_CALL_COUNT == null ? that.APPORTIONED_CALL_COUNT == null : this.APPORTIONED_CALL_COUNT.equals(that.APPORTIONED_CALL_COUNT));
    equal = equal && (this.APPORTIONED_DURATION_SECONDS == null ? that.APPORTIONED_DURATION_SECONDS == null : this.APPORTIONED_DURATION_SECONDS.equals(that.APPORTIONED_DURATION_SECONDS));
    equal = equal && (this.ACTUAL_USAGE == null ? that.ACTUAL_USAGE == null : this.ACTUAL_USAGE.equals(that.ACTUAL_USAGE));
    equal = equal && (this.CHARGED_USAGE == null ? that.CHARGED_USAGE == null : this.CHARGED_USAGE.equals(that.CHARGED_USAGE));
    equal = equal && (this.CHARGED_UNITS == null ? that.CHARGED_UNITS == null : this.CHARGED_UNITS.equals(that.CHARGED_UNITS));
    equal = equal && (this.EVENT_DURATION == null ? that.EVENT_DURATION == null : this.EVENT_DURATION.equals(that.EVENT_DURATION));
    equal = equal && (this.MINIMUM_CHARGE_ADJUSTMENT == null ? that.MINIMUM_CHARGE_ADJUSTMENT == null : this.MINIMUM_CHARGE_ADJUSTMENT.equals(that.MINIMUM_CHARGE_ADJUSTMENT));
    equal = equal && (this.MAXIMUM_CHARGE_ADJUSTMENT == null ? that.MAXIMUM_CHARGE_ADJUSTMENT == null : this.MAXIMUM_CHARGE_ADJUSTMENT.equals(that.MAXIMUM_CHARGE_ADJUSTMENT));
    equal = equal && (this.NETWORK_DURATION == null ? that.NETWORK_DURATION == null : this.NETWORK_DURATION.equals(that.NETWORK_DURATION));
    equal = equal && (this.DATA_VOLUME == null ? that.DATA_VOLUME == null : this.DATA_VOLUME.equals(that.DATA_VOLUME));
    equal = equal && (this.DATA_VOLUME_2 == null ? that.DATA_VOLUME_2 == null : this.DATA_VOLUME_2.equals(that.DATA_VOLUME_2));
    equal = equal && (this.DATA_VOLUME_3 == null ? that.DATA_VOLUME_3 == null : this.DATA_VOLUME_3.equals(that.DATA_VOLUME_3));
    equal = equal && (this.DATA_VOLUME_4 == null ? that.DATA_VOLUME_4 == null : this.DATA_VOLUME_4.equals(that.DATA_VOLUME_4));
    equal = equal && (this.REVENUE_SHARE_AMOUNT_1 == null ? that.REVENUE_SHARE_AMOUNT_1 == null : this.REVENUE_SHARE_AMOUNT_1.equals(that.REVENUE_SHARE_AMOUNT_1));
    equal = equal && (this.REVENUE_SHARE_AMOUNT_2 == null ? that.REVENUE_SHARE_AMOUNT_2 == null : this.REVENUE_SHARE_AMOUNT_2.equals(that.REVENUE_SHARE_AMOUNT_2));
    equal = equal && (this.REVENUE_SHARE_AMOUNT_3 == null ? that.REVENUE_SHARE_AMOUNT_3 == null : this.REVENUE_SHARE_AMOUNT_3.equals(that.REVENUE_SHARE_AMOUNT_3));
    equal = equal && (this.BASE_AMOUNT == null ? that.BASE_AMOUNT == null : this.BASE_AMOUNT.equals(that.BASE_AMOUNT));
    equal = equal && (this.AMOUNT == null ? that.AMOUNT == null : this.AMOUNT.equals(that.AMOUNT));
    equal = equal && (this.DLYS_DETAIL_ID == null ? that.DLYS_DETAIL_ID == null : this.DLYS_DETAIL_ID.equals(that.DLYS_DETAIL_ID));
    equal = equal && (this.TRAFFIC_PERIOD == null ? that.TRAFFIC_PERIOD == null : this.TRAFFIC_PERIOD.equals(that.TRAFFIC_PERIOD));
    equal = equal && (this.MESSAGE_DATE == null ? that.MESSAGE_DATE == null : this.MESSAGE_DATE.equals(that.MESSAGE_DATE));
    equal = equal && (this.BILLING_DATE == null ? that.BILLING_DATE == null : this.BILLING_DATE.equals(that.BILLING_DATE));
    equal = equal && (this.ADJUSTED_DATE == null ? that.ADJUSTED_DATE == null : this.ADJUSTED_DATE.equals(that.ADJUSTED_DATE));
    equal = equal && (this.PROCESS_DATE == null ? that.PROCESS_DATE == null : this.PROCESS_DATE.equals(that.PROCESS_DATE));
    equal = equal && (this.EVENT_START_DATE == null ? that.EVENT_START_DATE == null : this.EVENT_START_DATE.equals(that.EVENT_START_DATE));
    equal = equal && (this.EVENT_START_TIME == null ? that.EVENT_START_TIME == null : this.EVENT_START_TIME.equals(that.EVENT_START_TIME));
    equal = equal && (this.NETWORK_START_DATE == null ? that.NETWORK_START_DATE == null : this.NETWORK_START_DATE.equals(that.NETWORK_START_DATE));
    equal = equal && (this.NETWORK_START_TIME == null ? that.NETWORK_START_TIME == null : this.NETWORK_START_TIME.equals(that.NETWORK_START_TIME));
    equal = equal && (this.BILLING_START_TIME == null ? that.BILLING_START_TIME == null : this.BILLING_START_TIME.equals(that.BILLING_START_TIME));
    equal = equal && (this.BILLING_END_TIME == null ? that.BILLING_END_TIME == null : this.BILLING_END_TIME.equals(that.BILLING_END_TIME));
    equal = equal && (this.FLAT_RATE_CHARGE == null ? that.FLAT_RATE_CHARGE == null : this.FLAT_RATE_CHARGE.equals(that.FLAT_RATE_CHARGE));
    equal = equal && (this.RATE_STEP_FLAT_CHARGE == null ? that.RATE_STEP_FLAT_CHARGE == null : this.RATE_STEP_FLAT_CHARGE.equals(that.RATE_STEP_FLAT_CHARGE));
    equal = equal && (this.UNIT_COST_USED == null ? that.UNIT_COST_USED == null : this.UNIT_COST_USED.equals(that.UNIT_COST_USED));
    equal = equal && (this.CHARGE_BUNDLE == null ? that.CHARGE_BUNDLE == null : this.CHARGE_BUNDLE.equals(that.CHARGE_BUNDLE));
    equal = equal && (this.BASE_AMOUNT_FACTOR_USED == null ? that.BASE_AMOUNT_FACTOR_USED == null : this.BASE_AMOUNT_FACTOR_USED.equals(that.BASE_AMOUNT_FACTOR_USED));
    equal = equal && (this.REFUND_FACTOR == null ? that.REFUND_FACTOR == null : this.REFUND_FACTOR.equals(that.REFUND_FACTOR));
    equal = equal && (this.CURRENCY == null ? that.CURRENCY == null : this.CURRENCY.equals(that.CURRENCY));
    equal = equal && (this.CURRENCY_CONVERSION_RATE == null ? that.CURRENCY_CONVERSION_RATE == null : this.CURRENCY_CONVERSION_RATE.equals(that.CURRENCY_CONVERSION_RATE));
    equal = equal && (this.BASE_UNIT == null ? that.BASE_UNIT == null : this.BASE_UNIT.equals(that.BASE_UNIT));
    equal = equal && (this.RATE_UNIT == null ? that.RATE_UNIT == null : this.RATE_UNIT.equals(that.RATE_UNIT));
    equal = equal && (this.ROUNDED_UNIT_ID == null ? that.ROUNDED_UNIT_ID == null : this.ROUNDED_UNIT_ID.equals(that.ROUNDED_UNIT_ID));
    equal = equal && (this.RECORD_TYPE == null ? that.RECORD_TYPE == null : this.RECORD_TYPE.equals(that.RECORD_TYPE));
    equal = equal && (this.LINK_FIELD == null ? that.LINK_FIELD == null : this.LINK_FIELD.equals(that.LINK_FIELD));
    equal = equal && (this.REASON_FOR_CLEARDOWN == null ? that.REASON_FOR_CLEARDOWN == null : this.REASON_FOR_CLEARDOWN.equals(that.REASON_FOR_CLEARDOWN));
    equal = equal && (this.REPAIR_INDICATOR == null ? that.REPAIR_INDICATOR == null : this.REPAIR_INDICATOR.equals(that.REPAIR_INDICATOR));
    equal = equal && (this.RATED_BILLING_PERIOD == null ? that.RATED_BILLING_PERIOD == null : this.RATED_BILLING_PERIOD.equals(that.RATED_BILLING_PERIOD));
    equal = equal && (this.TRAFFIC_MOVEMENT_CTR == null ? that.TRAFFIC_MOVEMENT_CTR == null : this.TRAFFIC_MOVEMENT_CTR.equals(that.TRAFFIC_MOVEMENT_CTR));
    equal = equal && (this.RTE_LOOKUP_STYLE == null ? that.RTE_LOOKUP_STYLE == null : this.RTE_LOOKUP_STYLE.equals(that.RTE_LOOKUP_STYLE));
    equal = equal && (this.CDR_SOURCE == null ? that.CDR_SOURCE == null : this.CDR_SOURCE.equals(that.CDR_SOURCE));
    equal = equal && (this.CARRIER_DESTINATION_NAME == null ? that.CARRIER_DESTINATION_NAME == null : this.CARRIER_DESTINATION_NAME.equals(that.CARRIER_DESTINATION_NAME));
    equal = equal && (this.CARRIER_DESTINATION_ALIAS == null ? that.CARRIER_DESTINATION_ALIAS == null : this.CARRIER_DESTINATION_ALIAS.equals(that.CARRIER_DESTINATION_ALIAS));
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
    equal = equal && (this.CDR_FILE_NO == null ? that.CDR_FILE_NO == null : this.CDR_FILE_NO.equals(that.CDR_FILE_NO));
    equal = equal && (this.CDR_UCI_NO == null ? that.CDR_UCI_NO == null : this.CDR_UCI_NO.equals(that.CDR_UCI_NO));
    equal = equal && (this.CDR_UCI_ELEMENT == null ? that.CDR_UCI_ELEMENT == null : this.CDR_UCI_ELEMENT.equals(that.CDR_UCI_ELEMENT));
    equal = equal && (this.CDR_KEY_ID == null ? that.CDR_KEY_ID == null : this.CDR_KEY_ID.equals(that.CDR_KEY_ID));
    equal = equal && (this.CALL_SCENARIO == null ? that.CALL_SCENARIO == null : this.CALL_SCENARIO.equals(that.CALL_SCENARIO));
    equal = equal && (this.REMUNERATION_INDICATOR == null ? that.REMUNERATION_INDICATOR == null : this.REMUNERATION_INDICATOR.equals(that.REMUNERATION_INDICATOR));
    equal = equal && (this.BILLING_BASE_DIRECTION == null ? that.BILLING_BASE_DIRECTION == null : this.BILLING_BASE_DIRECTION.equals(that.BILLING_BASE_DIRECTION));
    equal = equal && (this.FRANCHISE == null ? that.FRANCHISE == null : this.FRANCHISE.equals(that.FRANCHISE));
    equal = equal && (this.PRODUCT_GROUP == null ? that.PRODUCT_GROUP == null : this.PRODUCT_GROUP.equals(that.PRODUCT_GROUP));
    equal = equal && (this.REVERSE_INDICATOR == null ? that.REVERSE_INDICATOR == null : this.REVERSE_INDICATOR.equals(that.REVERSE_INDICATOR));
    equal = equal && (this.EVENT_DIRECTION == null ? that.EVENT_DIRECTION == null : this.EVENT_DIRECTION.equals(that.EVENT_DIRECTION));
    equal = equal && (this.DERIVED_EVENT_DIRECTION == null ? that.DERIVED_EVENT_DIRECTION == null : this.DERIVED_EVENT_DIRECTION.equals(that.DERIVED_EVENT_DIRECTION));
    equal = equal && (this.BILLING_RATING_SCENARIO_TYPE == null ? that.BILLING_RATING_SCENARIO_TYPE == null : this.BILLING_RATING_SCENARIO_TYPE.equals(that.BILLING_RATING_SCENARIO_TYPE));
    equal = equal && (this.BILLED_PRODUCT == null ? that.BILLED_PRODUCT == null : this.BILLED_PRODUCT.equals(that.BILLED_PRODUCT));
    equal = equal && (this.RATING_SCENARIO == null ? that.RATING_SCENARIO == null : this.RATING_SCENARIO.equals(that.RATING_SCENARIO));
    equal = equal && (this.RATING_RULE_DEPENDENCY_IND == null ? that.RATING_RULE_DEPENDENCY_IND == null : this.RATING_RULE_DEPENDENCY_IND.equals(that.RATING_RULE_DEPENDENCY_IND));
    equal = equal && (this.ESTIMATE_INDICATOR == null ? that.ESTIMATE_INDICATOR == null : this.ESTIMATE_INDICATOR.equals(that.ESTIMATE_INDICATOR));
    equal = equal && (this.ACCOUNTING_METHOD == null ? that.ACCOUNTING_METHOD == null : this.ACCOUNTING_METHOD.equals(that.ACCOUNTING_METHOD));
    equal = equal && (this.CASH_FLOW == null ? that.CASH_FLOW == null : this.CASH_FLOW.equals(that.CASH_FLOW));
    equal = equal && (this.COMPONENT_DIRECTION == null ? that.COMPONENT_DIRECTION == null : this.COMPONENT_DIRECTION.equals(that.COMPONENT_DIRECTION));
    equal = equal && (this.STATEMENT_DIRECTION == null ? that.STATEMENT_DIRECTION == null : this.STATEMENT_DIRECTION.equals(that.STATEMENT_DIRECTION));
    equal = equal && (this.BILLING_METHOD == null ? that.BILLING_METHOD == null : this.BILLING_METHOD.equals(that.BILLING_METHOD));
    equal = equal && (this.RATING_COMPONENT == null ? that.RATING_COMPONENT == null : this.RATING_COMPONENT.equals(that.RATING_COMPONENT));
    equal = equal && (this.CALL_ATTEMPT_INDICATOR == null ? that.CALL_ATTEMPT_INDICATOR == null : this.CALL_ATTEMPT_INDICATOR.equals(that.CALL_ATTEMPT_INDICATOR));
    equal = equal && (this.PARAMETER_MATRIX == null ? that.PARAMETER_MATRIX == null : this.PARAMETER_MATRIX.equals(that.PARAMETER_MATRIX));
    equal = equal && (this.PARAMETER_MATRIX_LINE == null ? that.PARAMETER_MATRIX_LINE == null : this.PARAMETER_MATRIX_LINE.equals(that.PARAMETER_MATRIX_LINE));
    equal = equal && (this.STEP_NUMBER == null ? that.STEP_NUMBER == null : this.STEP_NUMBER.equals(that.STEP_NUMBER));
    equal = equal && (this.RATE_TYPE == null ? that.RATE_TYPE == null : this.RATE_TYPE.equals(that.RATE_TYPE));
    equal = equal && (this.TIME_PREMIUM == null ? that.TIME_PREMIUM == null : this.TIME_PREMIUM.equals(that.TIME_PREMIUM));
    equal = equal && (this.BILLING_OPERATOR == null ? that.BILLING_OPERATOR == null : this.BILLING_OPERATOR.equals(that.BILLING_OPERATOR));
    equal = equal && (this.RATE_OWNER_OPERATOR == null ? that.RATE_OWNER_OPERATOR == null : this.RATE_OWNER_OPERATOR.equals(that.RATE_OWNER_OPERATOR));
    equal = equal && (this.SETTLEMENT_OPERATOR == null ? that.SETTLEMENT_OPERATOR == null : this.SETTLEMENT_OPERATOR.equals(that.SETTLEMENT_OPERATOR));
    equal = equal && (this.RATE_NAME == null ? that.RATE_NAME == null : this.RATE_NAME.equals(that.RATE_NAME));
    equal = equal && (this.TIER == null ? that.TIER == null : this.TIER.equals(that.TIER));
    equal = equal && (this.TIER_GROUP == null ? that.TIER_GROUP == null : this.TIER_GROUP.equals(that.TIER_GROUP));
    equal = equal && (this.TIER_TYPE == null ? that.TIER_TYPE == null : this.TIER_TYPE.equals(that.TIER_TYPE));
    equal = equal && (this.OPTIMUM_POI == null ? that.OPTIMUM_POI == null : this.OPTIMUM_POI.equals(that.OPTIMUM_POI));
    equal = equal && (this.ADHOC_SUMMARY_IND == null ? that.ADHOC_SUMMARY_IND == null : this.ADHOC_SUMMARY_IND.equals(that.ADHOC_SUMMARY_IND));
    equal = equal && (this.BILLING_SUMMARY_IND == null ? that.BILLING_SUMMARY_IND == null : this.BILLING_SUMMARY_IND.equals(that.BILLING_SUMMARY_IND));
    equal = equal && (this.COMPLEMENTARY_SUMMARY_IND == null ? that.COMPLEMENTARY_SUMMARY_IND == null : this.COMPLEMENTARY_SUMMARY_IND.equals(that.COMPLEMENTARY_SUMMARY_IND));
    equal = equal && (this.TRAFFIC_SUMMARY_IND == null ? that.TRAFFIC_SUMMARY_IND == null : this.TRAFFIC_SUMMARY_IND.equals(that.TRAFFIC_SUMMARY_IND));
    equal = equal && (this.MINIMUM_CHARGE_IND == null ? that.MINIMUM_CHARGE_IND == null : this.MINIMUM_CHARGE_IND.equals(that.MINIMUM_CHARGE_IND));
    equal = equal && (this.CALL_DURATION_BUCKET == null ? that.CALL_DURATION_BUCKET == null : this.CALL_DURATION_BUCKET.equals(that.CALL_DURATION_BUCKET));
    equal = equal && (this.TIME_OF_DAY_BUCKET == null ? that.TIME_OF_DAY_BUCKET == null : this.TIME_OF_DAY_BUCKET.equals(that.TIME_OF_DAY_BUCKET));
    equal = equal && (this.USER_SUMMARISATION == null ? that.USER_SUMMARISATION == null : this.USER_SUMMARISATION.equals(that.USER_SUMMARISATION));
    equal = equal && (this.INCOMING_NODE == null ? that.INCOMING_NODE == null : this.INCOMING_NODE.equals(that.INCOMING_NODE));
    equal = equal && (this.INCOMING_PRODUCT == null ? that.INCOMING_PRODUCT == null : this.INCOMING_PRODUCT.equals(that.INCOMING_PRODUCT));
    equal = equal && (this.INCOMING_OPERATOR == null ? that.INCOMING_OPERATOR == null : this.INCOMING_OPERATOR.equals(that.INCOMING_OPERATOR));
    equal = equal && (this.INCOMING_PATH == null ? that.INCOMING_PATH == null : this.INCOMING_PATH.equals(that.INCOMING_PATH));
    equal = equal && (this.INCOMING_POI == null ? that.INCOMING_POI == null : this.INCOMING_POI.equals(that.INCOMING_POI));
    equal = equal && (this.OUTGOING_NODE == null ? that.OUTGOING_NODE == null : this.OUTGOING_NODE.equals(that.OUTGOING_NODE));
    equal = equal && (this.OUTGOING_PRODUCT == null ? that.OUTGOING_PRODUCT == null : this.OUTGOING_PRODUCT.equals(that.OUTGOING_PRODUCT));
    equal = equal && (this.OUTGOING_OPERATOR == null ? that.OUTGOING_OPERATOR == null : this.OUTGOING_OPERATOR.equals(that.OUTGOING_OPERATOR));
    equal = equal && (this.OUTGOING_PATH == null ? that.OUTGOING_PATH == null : this.OUTGOING_PATH.equals(that.OUTGOING_PATH));
    equal = equal && (this.OUTGOING_POI == null ? that.OUTGOING_POI == null : this.OUTGOING_POI.equals(that.OUTGOING_POI));
    equal = equal && (this.TRAFFIC_RATING_SCENARIO_TYPE == null ? that.TRAFFIC_RATING_SCENARIO_TYPE == null : this.TRAFFIC_RATING_SCENARIO_TYPE.equals(that.TRAFFIC_RATING_SCENARIO_TYPE));
    equal = equal && (this.TRAFFIC_ROUTE_TYPE == null ? that.TRAFFIC_ROUTE_TYPE == null : this.TRAFFIC_ROUTE_TYPE.equals(that.TRAFFIC_ROUTE_TYPE));
    equal = equal && (this.TRAFFIC_ROUTE == null ? that.TRAFFIC_ROUTE == null : this.TRAFFIC_ROUTE.equals(that.TRAFFIC_ROUTE));
    equal = equal && (this.TRAFFIC_AGREEMENT_OPERATOR == null ? that.TRAFFIC_AGREEMENT_OPERATOR == null : this.TRAFFIC_AGREEMENT_OPERATOR.equals(that.TRAFFIC_AGREEMENT_OPERATOR));
    equal = equal && (this.TRAFFIC_NEGOTIATION_DIR == null ? that.TRAFFIC_NEGOTIATION_DIR == null : this.TRAFFIC_NEGOTIATION_DIR.equals(that.TRAFFIC_NEGOTIATION_DIR));
    equal = equal && (this.BILLING_ROUTE_TYPE == null ? that.BILLING_ROUTE_TYPE == null : this.BILLING_ROUTE_TYPE.equals(that.BILLING_ROUTE_TYPE));
    equal = equal && (this.BILLING_ROUTE == null ? that.BILLING_ROUTE == null : this.BILLING_ROUTE.equals(that.BILLING_ROUTE));
    equal = equal && (this.BILLING_AGREEMENT_OPERATOR == null ? that.BILLING_AGREEMENT_OPERATOR == null : this.BILLING_AGREEMENT_OPERATOR.equals(that.BILLING_AGREEMENT_OPERATOR));
    equal = equal && (this.BILLING_NEGOTIATION_DIR == null ? that.BILLING_NEGOTIATION_DIR == null : this.BILLING_NEGOTIATION_DIR.equals(that.BILLING_NEGOTIATION_DIR));
    equal = equal && (this.ROUTE_IDENTIFIER == null ? that.ROUTE_IDENTIFIER == null : this.ROUTE_IDENTIFIER.equals(that.ROUTE_IDENTIFIER));
    equal = equal && (this.REFILE_INDICATOR == null ? that.REFILE_INDICATOR == null : this.REFILE_INDICATOR.equals(that.REFILE_INDICATOR));
    equal = equal && (this.AGREEMENT_NAAG_ANUM_LEVEL == null ? that.AGREEMENT_NAAG_ANUM_LEVEL == null : this.AGREEMENT_NAAG_ANUM_LEVEL.equals(that.AGREEMENT_NAAG_ANUM_LEVEL));
    equal = equal && (this.AGREEMENT_NAAG_ANUM == null ? that.AGREEMENT_NAAG_ANUM == null : this.AGREEMENT_NAAG_ANUM.equals(that.AGREEMENT_NAAG_ANUM));
    equal = equal && (this.AGREEMENT_NAAG_BNUM_LEVEL == null ? that.AGREEMENT_NAAG_BNUM_LEVEL == null : this.AGREEMENT_NAAG_BNUM_LEVEL.equals(that.AGREEMENT_NAAG_BNUM_LEVEL));
    equal = equal && (this.AGREEMENT_NAAG_BNUM == null ? that.AGREEMENT_NAAG_BNUM == null : this.AGREEMENT_NAAG_BNUM.equals(that.AGREEMENT_NAAG_BNUM));
    equal = equal && (this.WORLD_VIEW_NAAG_ANUM == null ? that.WORLD_VIEW_NAAG_ANUM == null : this.WORLD_VIEW_NAAG_ANUM.equals(that.WORLD_VIEW_NAAG_ANUM));
    equal = equal && (this.WORLD_VIEW_ANUM == null ? that.WORLD_VIEW_ANUM == null : this.WORLD_VIEW_ANUM.equals(that.WORLD_VIEW_ANUM));
    equal = equal && (this.WORLD_VIEW_NAAG_BNUM == null ? that.WORLD_VIEW_NAAG_BNUM == null : this.WORLD_VIEW_NAAG_BNUM.equals(that.WORLD_VIEW_NAAG_BNUM));
    equal = equal && (this.WORLD_VIEW_BNUM == null ? that.WORLD_VIEW_BNUM == null : this.WORLD_VIEW_BNUM.equals(that.WORLD_VIEW_BNUM));
    equal = equal && (this.TRAFFIC_BASE_DIRECTION == null ? that.TRAFFIC_BASE_DIRECTION == null : this.TRAFFIC_BASE_DIRECTION.equals(that.TRAFFIC_BASE_DIRECTION));
    equal = equal && (this.DATA_UNIT == null ? that.DATA_UNIT == null : this.DATA_UNIT.equals(that.DATA_UNIT));
    equal = equal && (this.DATA_UNIT_2 == null ? that.DATA_UNIT_2 == null : this.DATA_UNIT_2.equals(that.DATA_UNIT_2));
    equal = equal && (this.DATA_UNIT_3 == null ? that.DATA_UNIT_3 == null : this.DATA_UNIT_3.equals(that.DATA_UNIT_3));
    equal = equal && (this.DATA_UNIT_4 == null ? that.DATA_UNIT_4 == null : this.DATA_UNIT_4.equals(that.DATA_UNIT_4));
    equal = equal && (this.DISCRETE_RATING_PARAMETER_1 == null ? that.DISCRETE_RATING_PARAMETER_1 == null : this.DISCRETE_RATING_PARAMETER_1.equals(that.DISCRETE_RATING_PARAMETER_1));
    equal = equal && (this.DISCRETE_RATING_PARAMETER_2 == null ? that.DISCRETE_RATING_PARAMETER_2 == null : this.DISCRETE_RATING_PARAMETER_2.equals(that.DISCRETE_RATING_PARAMETER_2));
    equal = equal && (this.DISCRETE_RATING_PARAMETER_3 == null ? that.DISCRETE_RATING_PARAMETER_3 == null : this.DISCRETE_RATING_PARAMETER_3.equals(that.DISCRETE_RATING_PARAMETER_3));
    equal = equal && (this.REVENUE_SHARE_CURRENCY_1 == null ? that.REVENUE_SHARE_CURRENCY_1 == null : this.REVENUE_SHARE_CURRENCY_1.equals(that.REVENUE_SHARE_CURRENCY_1));
    equal = equal && (this.REVENUE_SHARE_CURRENCY_2 == null ? that.REVENUE_SHARE_CURRENCY_2 == null : this.REVENUE_SHARE_CURRENCY_2.equals(that.REVENUE_SHARE_CURRENCY_2));
    equal = equal && (this.REVENUE_SHARE_CURRENCY_3 == null ? that.REVENUE_SHARE_CURRENCY_3 == null : this.REVENUE_SHARE_CURRENCY_3.equals(that.REVENUE_SHARE_CURRENCY_3));
    equal = equal && (this.ANUM_OPERATOR == null ? that.ANUM_OPERATOR == null : this.ANUM_OPERATOR.equals(that.ANUM_OPERATOR));
    equal = equal && (this.ANUM_CNP == null ? that.ANUM_CNP == null : this.ANUM_CNP.equals(that.ANUM_CNP));
    equal = equal && (this.TRAFFIC_NAAG == null ? that.TRAFFIC_NAAG == null : this.TRAFFIC_NAAG.equals(that.TRAFFIC_NAAG));
    equal = equal && (this.NAAG_ANUM_LEVEL == null ? that.NAAG_ANUM_LEVEL == null : this.NAAG_ANUM_LEVEL.equals(that.NAAG_ANUM_LEVEL));
    equal = equal && (this.RECON_NAAG_ANUM == null ? that.RECON_NAAG_ANUM == null : this.RECON_NAAG_ANUM.equals(that.RECON_NAAG_ANUM));
    equal = equal && (this.NETWORK_ADDRESS_AGGR_ANUM == null ? that.NETWORK_ADDRESS_AGGR_ANUM == null : this.NETWORK_ADDRESS_AGGR_ANUM.equals(that.NETWORK_ADDRESS_AGGR_ANUM));
    equal = equal && (this.NETWORK_TYPE_ANUM == null ? that.NETWORK_TYPE_ANUM == null : this.NETWORK_TYPE_ANUM.equals(that.NETWORK_TYPE_ANUM));
    equal = equal && (this.BNUM_OPERATOR == null ? that.BNUM_OPERATOR == null : this.BNUM_OPERATOR.equals(that.BNUM_OPERATOR));
    equal = equal && (this.BNUM_CNP == null ? that.BNUM_CNP == null : this.BNUM_CNP.equals(that.BNUM_CNP));
    equal = equal && (this.NAAG_BNUM_LEVEL == null ? that.NAAG_BNUM_LEVEL == null : this.NAAG_BNUM_LEVEL.equals(that.NAAG_BNUM_LEVEL));
    equal = equal && (this.NETWORK_TYPE_BNUM == null ? that.NETWORK_TYPE_BNUM == null : this.NETWORK_TYPE_BNUM.equals(that.NETWORK_TYPE_BNUM));
    equal = equal && (this.RECON_NAAG_BNUM == null ? that.RECON_NAAG_BNUM == null : this.RECON_NAAG_BNUM.equals(that.RECON_NAAG_BNUM));
    equal = equal && (this.NETWORK_ADDRESS_AGGR_BNUM == null ? that.NETWORK_ADDRESS_AGGR_BNUM == null : this.NETWORK_ADDRESS_AGGR_BNUM.equals(that.NETWORK_ADDRESS_AGGR_BNUM));
    equal = equal && (this.DERIVED_PRODUCT_INDICATOR == null ? that.DERIVED_PRODUCT_INDICATOR == null : this.DERIVED_PRODUCT_INDICATOR.equals(that.DERIVED_PRODUCT_INDICATOR));
    equal = equal && (this.ANUM == null ? that.ANUM == null : this.ANUM.equals(that.ANUM));
    equal = equal && (this.BNUM == null ? that.BNUM == null : this.BNUM.equals(that.BNUM));
    equal = equal && (this.RECORD_SEQUENCE_NUMBER == null ? that.RECORD_SEQUENCE_NUMBER == null : this.RECORD_SEQUENCE_NUMBER.equals(that.RECORD_SEQUENCE_NUMBER));
    equal = equal && (this.USER_DATA == null ? that.USER_DATA == null : this.USER_DATA.equals(that.USER_DATA));
    equal = equal && (this.USER_DATA_2 == null ? that.USER_DATA_2 == null : this.USER_DATA_2.equals(that.USER_DATA_2));
    equal = equal && (this.USER_DATA_3 == null ? that.USER_DATA_3 == null : this.USER_DATA_3.equals(that.USER_DATA_3));
    equal = equal && (this.CALL_COUNT == null ? that.CALL_COUNT == null : this.CALL_COUNT.equals(that.CALL_COUNT));
    equal = equal && (this.START_CALL_COUNT == null ? that.START_CALL_COUNT == null : this.START_CALL_COUNT.equals(that.START_CALL_COUNT));
    equal = equal && (this.RATE_STEP_CALL_COUNT == null ? that.RATE_STEP_CALL_COUNT == null : this.RATE_STEP_CALL_COUNT.equals(that.RATE_STEP_CALL_COUNT));
    equal = equal && (this.APPORTIONED_CALL_COUNT == null ? that.APPORTIONED_CALL_COUNT == null : this.APPORTIONED_CALL_COUNT.equals(that.APPORTIONED_CALL_COUNT));
    equal = equal && (this.APPORTIONED_DURATION_SECONDS == null ? that.APPORTIONED_DURATION_SECONDS == null : this.APPORTIONED_DURATION_SECONDS.equals(that.APPORTIONED_DURATION_SECONDS));
    equal = equal && (this.ACTUAL_USAGE == null ? that.ACTUAL_USAGE == null : this.ACTUAL_USAGE.equals(that.ACTUAL_USAGE));
    equal = equal && (this.CHARGED_USAGE == null ? that.CHARGED_USAGE == null : this.CHARGED_USAGE.equals(that.CHARGED_USAGE));
    equal = equal && (this.CHARGED_UNITS == null ? that.CHARGED_UNITS == null : this.CHARGED_UNITS.equals(that.CHARGED_UNITS));
    equal = equal && (this.EVENT_DURATION == null ? that.EVENT_DURATION == null : this.EVENT_DURATION.equals(that.EVENT_DURATION));
    equal = equal && (this.MINIMUM_CHARGE_ADJUSTMENT == null ? that.MINIMUM_CHARGE_ADJUSTMENT == null : this.MINIMUM_CHARGE_ADJUSTMENT.equals(that.MINIMUM_CHARGE_ADJUSTMENT));
    equal = equal && (this.MAXIMUM_CHARGE_ADJUSTMENT == null ? that.MAXIMUM_CHARGE_ADJUSTMENT == null : this.MAXIMUM_CHARGE_ADJUSTMENT.equals(that.MAXIMUM_CHARGE_ADJUSTMENT));
    equal = equal && (this.NETWORK_DURATION == null ? that.NETWORK_DURATION == null : this.NETWORK_DURATION.equals(that.NETWORK_DURATION));
    equal = equal && (this.DATA_VOLUME == null ? that.DATA_VOLUME == null : this.DATA_VOLUME.equals(that.DATA_VOLUME));
    equal = equal && (this.DATA_VOLUME_2 == null ? that.DATA_VOLUME_2 == null : this.DATA_VOLUME_2.equals(that.DATA_VOLUME_2));
    equal = equal && (this.DATA_VOLUME_3 == null ? that.DATA_VOLUME_3 == null : this.DATA_VOLUME_3.equals(that.DATA_VOLUME_3));
    equal = equal && (this.DATA_VOLUME_4 == null ? that.DATA_VOLUME_4 == null : this.DATA_VOLUME_4.equals(that.DATA_VOLUME_4));
    equal = equal && (this.REVENUE_SHARE_AMOUNT_1 == null ? that.REVENUE_SHARE_AMOUNT_1 == null : this.REVENUE_SHARE_AMOUNT_1.equals(that.REVENUE_SHARE_AMOUNT_1));
    equal = equal && (this.REVENUE_SHARE_AMOUNT_2 == null ? that.REVENUE_SHARE_AMOUNT_2 == null : this.REVENUE_SHARE_AMOUNT_2.equals(that.REVENUE_SHARE_AMOUNT_2));
    equal = equal && (this.REVENUE_SHARE_AMOUNT_3 == null ? that.REVENUE_SHARE_AMOUNT_3 == null : this.REVENUE_SHARE_AMOUNT_3.equals(that.REVENUE_SHARE_AMOUNT_3));
    equal = equal && (this.BASE_AMOUNT == null ? that.BASE_AMOUNT == null : this.BASE_AMOUNT.equals(that.BASE_AMOUNT));
    equal = equal && (this.AMOUNT == null ? that.AMOUNT == null : this.AMOUNT.equals(that.AMOUNT));
    equal = equal && (this.DLYS_DETAIL_ID == null ? that.DLYS_DETAIL_ID == null : this.DLYS_DETAIL_ID.equals(that.DLYS_DETAIL_ID));
    equal = equal && (this.TRAFFIC_PERIOD == null ? that.TRAFFIC_PERIOD == null : this.TRAFFIC_PERIOD.equals(that.TRAFFIC_PERIOD));
    equal = equal && (this.MESSAGE_DATE == null ? that.MESSAGE_DATE == null : this.MESSAGE_DATE.equals(that.MESSAGE_DATE));
    equal = equal && (this.BILLING_DATE == null ? that.BILLING_DATE == null : this.BILLING_DATE.equals(that.BILLING_DATE));
    equal = equal && (this.ADJUSTED_DATE == null ? that.ADJUSTED_DATE == null : this.ADJUSTED_DATE.equals(that.ADJUSTED_DATE));
    equal = equal && (this.PROCESS_DATE == null ? that.PROCESS_DATE == null : this.PROCESS_DATE.equals(that.PROCESS_DATE));
    equal = equal && (this.EVENT_START_DATE == null ? that.EVENT_START_DATE == null : this.EVENT_START_DATE.equals(that.EVENT_START_DATE));
    equal = equal && (this.EVENT_START_TIME == null ? that.EVENT_START_TIME == null : this.EVENT_START_TIME.equals(that.EVENT_START_TIME));
    equal = equal && (this.NETWORK_START_DATE == null ? that.NETWORK_START_DATE == null : this.NETWORK_START_DATE.equals(that.NETWORK_START_DATE));
    equal = equal && (this.NETWORK_START_TIME == null ? that.NETWORK_START_TIME == null : this.NETWORK_START_TIME.equals(that.NETWORK_START_TIME));
    equal = equal && (this.BILLING_START_TIME == null ? that.BILLING_START_TIME == null : this.BILLING_START_TIME.equals(that.BILLING_START_TIME));
    equal = equal && (this.BILLING_END_TIME == null ? that.BILLING_END_TIME == null : this.BILLING_END_TIME.equals(that.BILLING_END_TIME));
    equal = equal && (this.FLAT_RATE_CHARGE == null ? that.FLAT_RATE_CHARGE == null : this.FLAT_RATE_CHARGE.equals(that.FLAT_RATE_CHARGE));
    equal = equal && (this.RATE_STEP_FLAT_CHARGE == null ? that.RATE_STEP_FLAT_CHARGE == null : this.RATE_STEP_FLAT_CHARGE.equals(that.RATE_STEP_FLAT_CHARGE));
    equal = equal && (this.UNIT_COST_USED == null ? that.UNIT_COST_USED == null : this.UNIT_COST_USED.equals(that.UNIT_COST_USED));
    equal = equal && (this.CHARGE_BUNDLE == null ? that.CHARGE_BUNDLE == null : this.CHARGE_BUNDLE.equals(that.CHARGE_BUNDLE));
    equal = equal && (this.BASE_AMOUNT_FACTOR_USED == null ? that.BASE_AMOUNT_FACTOR_USED == null : this.BASE_AMOUNT_FACTOR_USED.equals(that.BASE_AMOUNT_FACTOR_USED));
    equal = equal && (this.REFUND_FACTOR == null ? that.REFUND_FACTOR == null : this.REFUND_FACTOR.equals(that.REFUND_FACTOR));
    equal = equal && (this.CURRENCY == null ? that.CURRENCY == null : this.CURRENCY.equals(that.CURRENCY));
    equal = equal && (this.CURRENCY_CONVERSION_RATE == null ? that.CURRENCY_CONVERSION_RATE == null : this.CURRENCY_CONVERSION_RATE.equals(that.CURRENCY_CONVERSION_RATE));
    equal = equal && (this.BASE_UNIT == null ? that.BASE_UNIT == null : this.BASE_UNIT.equals(that.BASE_UNIT));
    equal = equal && (this.RATE_UNIT == null ? that.RATE_UNIT == null : this.RATE_UNIT.equals(that.RATE_UNIT));
    equal = equal && (this.ROUNDED_UNIT_ID == null ? that.ROUNDED_UNIT_ID == null : this.ROUNDED_UNIT_ID.equals(that.ROUNDED_UNIT_ID));
    equal = equal && (this.RECORD_TYPE == null ? that.RECORD_TYPE == null : this.RECORD_TYPE.equals(that.RECORD_TYPE));
    equal = equal && (this.LINK_FIELD == null ? that.LINK_FIELD == null : this.LINK_FIELD.equals(that.LINK_FIELD));
    equal = equal && (this.REASON_FOR_CLEARDOWN == null ? that.REASON_FOR_CLEARDOWN == null : this.REASON_FOR_CLEARDOWN.equals(that.REASON_FOR_CLEARDOWN));
    equal = equal && (this.REPAIR_INDICATOR == null ? that.REPAIR_INDICATOR == null : this.REPAIR_INDICATOR.equals(that.REPAIR_INDICATOR));
    equal = equal && (this.RATED_BILLING_PERIOD == null ? that.RATED_BILLING_PERIOD == null : this.RATED_BILLING_PERIOD.equals(that.RATED_BILLING_PERIOD));
    equal = equal && (this.TRAFFIC_MOVEMENT_CTR == null ? that.TRAFFIC_MOVEMENT_CTR == null : this.TRAFFIC_MOVEMENT_CTR.equals(that.TRAFFIC_MOVEMENT_CTR));
    equal = equal && (this.RTE_LOOKUP_STYLE == null ? that.RTE_LOOKUP_STYLE == null : this.RTE_LOOKUP_STYLE.equals(that.RTE_LOOKUP_STYLE));
    equal = equal && (this.CDR_SOURCE == null ? that.CDR_SOURCE == null : this.CDR_SOURCE.equals(that.CDR_SOURCE));
    equal = equal && (this.CARRIER_DESTINATION_NAME == null ? that.CARRIER_DESTINATION_NAME == null : this.CARRIER_DESTINATION_NAME.equals(that.CARRIER_DESTINATION_NAME));
    equal = equal && (this.CARRIER_DESTINATION_ALIAS == null ? that.CARRIER_DESTINATION_ALIAS == null : this.CARRIER_DESTINATION_ALIAS.equals(that.CARRIER_DESTINATION_ALIAS));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.CDR_FILE_NO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.CDR_UCI_NO = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.CDR_UCI_ELEMENT = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.CDR_KEY_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.CALL_SCENARIO = JdbcWritableBridge.readString(5, __dbResults);
    this.REMUNERATION_INDICATOR = JdbcWritableBridge.readString(6, __dbResults);
    this.BILLING_BASE_DIRECTION = JdbcWritableBridge.readString(7, __dbResults);
    this.FRANCHISE = JdbcWritableBridge.readString(8, __dbResults);
    this.PRODUCT_GROUP = JdbcWritableBridge.readString(9, __dbResults);
    this.REVERSE_INDICATOR = JdbcWritableBridge.readString(10, __dbResults);
    this.EVENT_DIRECTION = JdbcWritableBridge.readString(11, __dbResults);
    this.DERIVED_EVENT_DIRECTION = JdbcWritableBridge.readString(12, __dbResults);
    this.BILLING_RATING_SCENARIO_TYPE = JdbcWritableBridge.readString(13, __dbResults);
    this.BILLED_PRODUCT = JdbcWritableBridge.readString(14, __dbResults);
    this.RATING_SCENARIO = JdbcWritableBridge.readString(15, __dbResults);
    this.RATING_RULE_DEPENDENCY_IND = JdbcWritableBridge.readString(16, __dbResults);
    this.ESTIMATE_INDICATOR = JdbcWritableBridge.readString(17, __dbResults);
    this.ACCOUNTING_METHOD = JdbcWritableBridge.readString(18, __dbResults);
    this.CASH_FLOW = JdbcWritableBridge.readString(19, __dbResults);
    this.COMPONENT_DIRECTION = JdbcWritableBridge.readString(20, __dbResults);
    this.STATEMENT_DIRECTION = JdbcWritableBridge.readString(21, __dbResults);
    this.BILLING_METHOD = JdbcWritableBridge.readString(22, __dbResults);
    this.RATING_COMPONENT = JdbcWritableBridge.readString(23, __dbResults);
    this.CALL_ATTEMPT_INDICATOR = JdbcWritableBridge.readString(24, __dbResults);
    this.PARAMETER_MATRIX = JdbcWritableBridge.readString(25, __dbResults);
    this.PARAMETER_MATRIX_LINE = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.STEP_NUMBER = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.RATE_TYPE = JdbcWritableBridge.readString(28, __dbResults);
    this.TIME_PREMIUM = JdbcWritableBridge.readString(29, __dbResults);
    this.BILLING_OPERATOR = JdbcWritableBridge.readString(30, __dbResults);
    this.RATE_OWNER_OPERATOR = JdbcWritableBridge.readString(31, __dbResults);
    this.SETTLEMENT_OPERATOR = JdbcWritableBridge.readString(32, __dbResults);
    this.RATE_NAME = JdbcWritableBridge.readString(33, __dbResults);
    this.TIER = JdbcWritableBridge.readString(34, __dbResults);
    this.TIER_GROUP = JdbcWritableBridge.readString(35, __dbResults);
    this.TIER_TYPE = JdbcWritableBridge.readString(36, __dbResults);
    this.OPTIMUM_POI = JdbcWritableBridge.readString(37, __dbResults);
    this.ADHOC_SUMMARY_IND = JdbcWritableBridge.readString(38, __dbResults);
    this.BILLING_SUMMARY_IND = JdbcWritableBridge.readString(39, __dbResults);
    this.COMPLEMENTARY_SUMMARY_IND = JdbcWritableBridge.readString(40, __dbResults);
    this.TRAFFIC_SUMMARY_IND = JdbcWritableBridge.readString(41, __dbResults);
    this.MINIMUM_CHARGE_IND = JdbcWritableBridge.readString(42, __dbResults);
    this.CALL_DURATION_BUCKET = JdbcWritableBridge.readBigDecimal(43, __dbResults);
    this.TIME_OF_DAY_BUCKET = JdbcWritableBridge.readBigDecimal(44, __dbResults);
    this.USER_SUMMARISATION = JdbcWritableBridge.readString(45, __dbResults);
    this.INCOMING_NODE = JdbcWritableBridge.readString(46, __dbResults);
    this.INCOMING_PRODUCT = JdbcWritableBridge.readString(47, __dbResults);
    this.INCOMING_OPERATOR = JdbcWritableBridge.readString(48, __dbResults);
    this.INCOMING_PATH = JdbcWritableBridge.readString(49, __dbResults);
    this.INCOMING_POI = JdbcWritableBridge.readString(50, __dbResults);
    this.OUTGOING_NODE = JdbcWritableBridge.readString(51, __dbResults);
    this.OUTGOING_PRODUCT = JdbcWritableBridge.readString(52, __dbResults);
    this.OUTGOING_OPERATOR = JdbcWritableBridge.readString(53, __dbResults);
    this.OUTGOING_PATH = JdbcWritableBridge.readString(54, __dbResults);
    this.OUTGOING_POI = JdbcWritableBridge.readString(55, __dbResults);
    this.TRAFFIC_RATING_SCENARIO_TYPE = JdbcWritableBridge.readString(56, __dbResults);
    this.TRAFFIC_ROUTE_TYPE = JdbcWritableBridge.readString(57, __dbResults);
    this.TRAFFIC_ROUTE = JdbcWritableBridge.readString(58, __dbResults);
    this.TRAFFIC_AGREEMENT_OPERATOR = JdbcWritableBridge.readString(59, __dbResults);
    this.TRAFFIC_NEGOTIATION_DIR = JdbcWritableBridge.readString(60, __dbResults);
    this.BILLING_ROUTE_TYPE = JdbcWritableBridge.readString(61, __dbResults);
    this.BILLING_ROUTE = JdbcWritableBridge.readString(62, __dbResults);
    this.BILLING_AGREEMENT_OPERATOR = JdbcWritableBridge.readString(63, __dbResults);
    this.BILLING_NEGOTIATION_DIR = JdbcWritableBridge.readString(64, __dbResults);
    this.ROUTE_IDENTIFIER = JdbcWritableBridge.readString(65, __dbResults);
    this.REFILE_INDICATOR = JdbcWritableBridge.readString(66, __dbResults);
    this.AGREEMENT_NAAG_ANUM_LEVEL = JdbcWritableBridge.readString(67, __dbResults);
    this.AGREEMENT_NAAG_ANUM = JdbcWritableBridge.readString(68, __dbResults);
    this.AGREEMENT_NAAG_BNUM_LEVEL = JdbcWritableBridge.readString(69, __dbResults);
    this.AGREEMENT_NAAG_BNUM = JdbcWritableBridge.readString(70, __dbResults);
    this.WORLD_VIEW_NAAG_ANUM = JdbcWritableBridge.readString(71, __dbResults);
    this.WORLD_VIEW_ANUM = JdbcWritableBridge.readString(72, __dbResults);
    this.WORLD_VIEW_NAAG_BNUM = JdbcWritableBridge.readString(73, __dbResults);
    this.WORLD_VIEW_BNUM = JdbcWritableBridge.readString(74, __dbResults);
    this.TRAFFIC_BASE_DIRECTION = JdbcWritableBridge.readString(75, __dbResults);
    this.DATA_UNIT = JdbcWritableBridge.readString(76, __dbResults);
    this.DATA_UNIT_2 = JdbcWritableBridge.readString(77, __dbResults);
    this.DATA_UNIT_3 = JdbcWritableBridge.readString(78, __dbResults);
    this.DATA_UNIT_4 = JdbcWritableBridge.readString(79, __dbResults);
    this.DISCRETE_RATING_PARAMETER_1 = JdbcWritableBridge.readString(80, __dbResults);
    this.DISCRETE_RATING_PARAMETER_2 = JdbcWritableBridge.readString(81, __dbResults);
    this.DISCRETE_RATING_PARAMETER_3 = JdbcWritableBridge.readString(82, __dbResults);
    this.REVENUE_SHARE_CURRENCY_1 = JdbcWritableBridge.readString(83, __dbResults);
    this.REVENUE_SHARE_CURRENCY_2 = JdbcWritableBridge.readString(84, __dbResults);
    this.REVENUE_SHARE_CURRENCY_3 = JdbcWritableBridge.readString(85, __dbResults);
    this.ANUM_OPERATOR = JdbcWritableBridge.readString(86, __dbResults);
    this.ANUM_CNP = JdbcWritableBridge.readString(87, __dbResults);
    this.TRAFFIC_NAAG = JdbcWritableBridge.readString(88, __dbResults);
    this.NAAG_ANUM_LEVEL = JdbcWritableBridge.readString(89, __dbResults);
    this.RECON_NAAG_ANUM = JdbcWritableBridge.readString(90, __dbResults);
    this.NETWORK_ADDRESS_AGGR_ANUM = JdbcWritableBridge.readString(91, __dbResults);
    this.NETWORK_TYPE_ANUM = JdbcWritableBridge.readString(92, __dbResults);
    this.BNUM_OPERATOR = JdbcWritableBridge.readString(93, __dbResults);
    this.BNUM_CNP = JdbcWritableBridge.readString(94, __dbResults);
    this.NAAG_BNUM_LEVEL = JdbcWritableBridge.readString(95, __dbResults);
    this.NETWORK_TYPE_BNUM = JdbcWritableBridge.readString(96, __dbResults);
    this.RECON_NAAG_BNUM = JdbcWritableBridge.readString(97, __dbResults);
    this.NETWORK_ADDRESS_AGGR_BNUM = JdbcWritableBridge.readString(98, __dbResults);
    this.DERIVED_PRODUCT_INDICATOR = JdbcWritableBridge.readString(99, __dbResults);
    this.ANUM = JdbcWritableBridge.readString(100, __dbResults);
    this.BNUM = JdbcWritableBridge.readString(101, __dbResults);
    this.RECORD_SEQUENCE_NUMBER = JdbcWritableBridge.readString(102, __dbResults);
    this.USER_DATA = JdbcWritableBridge.readString(103, __dbResults);
    this.USER_DATA_2 = JdbcWritableBridge.readString(104, __dbResults);
    this.USER_DATA_3 = JdbcWritableBridge.readString(105, __dbResults);
    this.CALL_COUNT = JdbcWritableBridge.readBigDecimal(106, __dbResults);
    this.START_CALL_COUNT = JdbcWritableBridge.readBigDecimal(107, __dbResults);
    this.RATE_STEP_CALL_COUNT = JdbcWritableBridge.readBigDecimal(108, __dbResults);
    this.APPORTIONED_CALL_COUNT = JdbcWritableBridge.readBigDecimal(109, __dbResults);
    this.APPORTIONED_DURATION_SECONDS = JdbcWritableBridge.readBigDecimal(110, __dbResults);
    this.ACTUAL_USAGE = JdbcWritableBridge.readBigDecimal(111, __dbResults);
    this.CHARGED_USAGE = JdbcWritableBridge.readBigDecimal(112, __dbResults);
    this.CHARGED_UNITS = JdbcWritableBridge.readString(113, __dbResults);
    this.EVENT_DURATION = JdbcWritableBridge.readBigDecimal(114, __dbResults);
    this.MINIMUM_CHARGE_ADJUSTMENT = JdbcWritableBridge.readBigDecimal(115, __dbResults);
    this.MAXIMUM_CHARGE_ADJUSTMENT = JdbcWritableBridge.readBigDecimal(116, __dbResults);
    this.NETWORK_DURATION = JdbcWritableBridge.readBigDecimal(117, __dbResults);
    this.DATA_VOLUME = JdbcWritableBridge.readBigDecimal(118, __dbResults);
    this.DATA_VOLUME_2 = JdbcWritableBridge.readBigDecimal(119, __dbResults);
    this.DATA_VOLUME_3 = JdbcWritableBridge.readBigDecimal(120, __dbResults);
    this.DATA_VOLUME_4 = JdbcWritableBridge.readBigDecimal(121, __dbResults);
    this.REVENUE_SHARE_AMOUNT_1 = JdbcWritableBridge.readBigDecimal(122, __dbResults);
    this.REVENUE_SHARE_AMOUNT_2 = JdbcWritableBridge.readBigDecimal(123, __dbResults);
    this.REVENUE_SHARE_AMOUNT_3 = JdbcWritableBridge.readBigDecimal(124, __dbResults);
    this.BASE_AMOUNT = JdbcWritableBridge.readBigDecimal(125, __dbResults);
    this.AMOUNT = JdbcWritableBridge.readBigDecimal(126, __dbResults);
    this.DLYS_DETAIL_ID = JdbcWritableBridge.readString(127, __dbResults);
    this.TRAFFIC_PERIOD = JdbcWritableBridge.readString(128, __dbResults);
    this.MESSAGE_DATE = JdbcWritableBridge.readTimestamp(129, __dbResults);
    this.BILLING_DATE = JdbcWritableBridge.readTimestamp(130, __dbResults);
    this.ADJUSTED_DATE = JdbcWritableBridge.readTimestamp(131, __dbResults);
    this.PROCESS_DATE = JdbcWritableBridge.readTimestamp(132, __dbResults);
    this.EVENT_START_DATE = JdbcWritableBridge.readTimestamp(133, __dbResults);
    this.EVENT_START_TIME = JdbcWritableBridge.readString(134, __dbResults);
    this.NETWORK_START_DATE = JdbcWritableBridge.readBigDecimal(135, __dbResults);
    this.NETWORK_START_TIME = JdbcWritableBridge.readBigDecimal(136, __dbResults);
    this.BILLING_START_TIME = JdbcWritableBridge.readBigDecimal(137, __dbResults);
    this.BILLING_END_TIME = JdbcWritableBridge.readBigDecimal(138, __dbResults);
    this.FLAT_RATE_CHARGE = JdbcWritableBridge.readString(139, __dbResults);
    this.RATE_STEP_FLAT_CHARGE = JdbcWritableBridge.readBigDecimal(140, __dbResults);
    this.UNIT_COST_USED = JdbcWritableBridge.readBigDecimal(141, __dbResults);
    this.CHARGE_BUNDLE = JdbcWritableBridge.readBigDecimal(142, __dbResults);
    this.BASE_AMOUNT_FACTOR_USED = JdbcWritableBridge.readBigDecimal(143, __dbResults);
    this.REFUND_FACTOR = JdbcWritableBridge.readBigDecimal(144, __dbResults);
    this.CURRENCY = JdbcWritableBridge.readString(145, __dbResults);
    this.CURRENCY_CONVERSION_RATE = JdbcWritableBridge.readBigDecimal(146, __dbResults);
    this.BASE_UNIT = JdbcWritableBridge.readString(147, __dbResults);
    this.RATE_UNIT = JdbcWritableBridge.readString(148, __dbResults);
    this.ROUNDED_UNIT_ID = JdbcWritableBridge.readBigDecimal(149, __dbResults);
    this.RECORD_TYPE = JdbcWritableBridge.readBigDecimal(150, __dbResults);
    this.LINK_FIELD = JdbcWritableBridge.readBigDecimal(151, __dbResults);
    this.REASON_FOR_CLEARDOWN = JdbcWritableBridge.readBigDecimal(152, __dbResults);
    this.REPAIR_INDICATOR = JdbcWritableBridge.readBigDecimal(153, __dbResults);
    this.RATED_BILLING_PERIOD = JdbcWritableBridge.readBigDecimal(154, __dbResults);
    this.TRAFFIC_MOVEMENT_CTR = JdbcWritableBridge.readBigDecimal(155, __dbResults);
    this.RTE_LOOKUP_STYLE = JdbcWritableBridge.readString(156, __dbResults);
    this.CDR_SOURCE = JdbcWritableBridge.readString(157, __dbResults);
    this.CARRIER_DESTINATION_NAME = JdbcWritableBridge.readString(158, __dbResults);
    this.CARRIER_DESTINATION_ALIAS = JdbcWritableBridge.readString(159, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.CDR_FILE_NO = JdbcWritableBridge.readBigDecimal(1, __dbResults);
    this.CDR_UCI_NO = JdbcWritableBridge.readBigDecimal(2, __dbResults);
    this.CDR_UCI_ELEMENT = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.CDR_KEY_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.CALL_SCENARIO = JdbcWritableBridge.readString(5, __dbResults);
    this.REMUNERATION_INDICATOR = JdbcWritableBridge.readString(6, __dbResults);
    this.BILLING_BASE_DIRECTION = JdbcWritableBridge.readString(7, __dbResults);
    this.FRANCHISE = JdbcWritableBridge.readString(8, __dbResults);
    this.PRODUCT_GROUP = JdbcWritableBridge.readString(9, __dbResults);
    this.REVERSE_INDICATOR = JdbcWritableBridge.readString(10, __dbResults);
    this.EVENT_DIRECTION = JdbcWritableBridge.readString(11, __dbResults);
    this.DERIVED_EVENT_DIRECTION = JdbcWritableBridge.readString(12, __dbResults);
    this.BILLING_RATING_SCENARIO_TYPE = JdbcWritableBridge.readString(13, __dbResults);
    this.BILLED_PRODUCT = JdbcWritableBridge.readString(14, __dbResults);
    this.RATING_SCENARIO = JdbcWritableBridge.readString(15, __dbResults);
    this.RATING_RULE_DEPENDENCY_IND = JdbcWritableBridge.readString(16, __dbResults);
    this.ESTIMATE_INDICATOR = JdbcWritableBridge.readString(17, __dbResults);
    this.ACCOUNTING_METHOD = JdbcWritableBridge.readString(18, __dbResults);
    this.CASH_FLOW = JdbcWritableBridge.readString(19, __dbResults);
    this.COMPONENT_DIRECTION = JdbcWritableBridge.readString(20, __dbResults);
    this.STATEMENT_DIRECTION = JdbcWritableBridge.readString(21, __dbResults);
    this.BILLING_METHOD = JdbcWritableBridge.readString(22, __dbResults);
    this.RATING_COMPONENT = JdbcWritableBridge.readString(23, __dbResults);
    this.CALL_ATTEMPT_INDICATOR = JdbcWritableBridge.readString(24, __dbResults);
    this.PARAMETER_MATRIX = JdbcWritableBridge.readString(25, __dbResults);
    this.PARAMETER_MATRIX_LINE = JdbcWritableBridge.readBigDecimal(26, __dbResults);
    this.STEP_NUMBER = JdbcWritableBridge.readBigDecimal(27, __dbResults);
    this.RATE_TYPE = JdbcWritableBridge.readString(28, __dbResults);
    this.TIME_PREMIUM = JdbcWritableBridge.readString(29, __dbResults);
    this.BILLING_OPERATOR = JdbcWritableBridge.readString(30, __dbResults);
    this.RATE_OWNER_OPERATOR = JdbcWritableBridge.readString(31, __dbResults);
    this.SETTLEMENT_OPERATOR = JdbcWritableBridge.readString(32, __dbResults);
    this.RATE_NAME = JdbcWritableBridge.readString(33, __dbResults);
    this.TIER = JdbcWritableBridge.readString(34, __dbResults);
    this.TIER_GROUP = JdbcWritableBridge.readString(35, __dbResults);
    this.TIER_TYPE = JdbcWritableBridge.readString(36, __dbResults);
    this.OPTIMUM_POI = JdbcWritableBridge.readString(37, __dbResults);
    this.ADHOC_SUMMARY_IND = JdbcWritableBridge.readString(38, __dbResults);
    this.BILLING_SUMMARY_IND = JdbcWritableBridge.readString(39, __dbResults);
    this.COMPLEMENTARY_SUMMARY_IND = JdbcWritableBridge.readString(40, __dbResults);
    this.TRAFFIC_SUMMARY_IND = JdbcWritableBridge.readString(41, __dbResults);
    this.MINIMUM_CHARGE_IND = JdbcWritableBridge.readString(42, __dbResults);
    this.CALL_DURATION_BUCKET = JdbcWritableBridge.readBigDecimal(43, __dbResults);
    this.TIME_OF_DAY_BUCKET = JdbcWritableBridge.readBigDecimal(44, __dbResults);
    this.USER_SUMMARISATION = JdbcWritableBridge.readString(45, __dbResults);
    this.INCOMING_NODE = JdbcWritableBridge.readString(46, __dbResults);
    this.INCOMING_PRODUCT = JdbcWritableBridge.readString(47, __dbResults);
    this.INCOMING_OPERATOR = JdbcWritableBridge.readString(48, __dbResults);
    this.INCOMING_PATH = JdbcWritableBridge.readString(49, __dbResults);
    this.INCOMING_POI = JdbcWritableBridge.readString(50, __dbResults);
    this.OUTGOING_NODE = JdbcWritableBridge.readString(51, __dbResults);
    this.OUTGOING_PRODUCT = JdbcWritableBridge.readString(52, __dbResults);
    this.OUTGOING_OPERATOR = JdbcWritableBridge.readString(53, __dbResults);
    this.OUTGOING_PATH = JdbcWritableBridge.readString(54, __dbResults);
    this.OUTGOING_POI = JdbcWritableBridge.readString(55, __dbResults);
    this.TRAFFIC_RATING_SCENARIO_TYPE = JdbcWritableBridge.readString(56, __dbResults);
    this.TRAFFIC_ROUTE_TYPE = JdbcWritableBridge.readString(57, __dbResults);
    this.TRAFFIC_ROUTE = JdbcWritableBridge.readString(58, __dbResults);
    this.TRAFFIC_AGREEMENT_OPERATOR = JdbcWritableBridge.readString(59, __dbResults);
    this.TRAFFIC_NEGOTIATION_DIR = JdbcWritableBridge.readString(60, __dbResults);
    this.BILLING_ROUTE_TYPE = JdbcWritableBridge.readString(61, __dbResults);
    this.BILLING_ROUTE = JdbcWritableBridge.readString(62, __dbResults);
    this.BILLING_AGREEMENT_OPERATOR = JdbcWritableBridge.readString(63, __dbResults);
    this.BILLING_NEGOTIATION_DIR = JdbcWritableBridge.readString(64, __dbResults);
    this.ROUTE_IDENTIFIER = JdbcWritableBridge.readString(65, __dbResults);
    this.REFILE_INDICATOR = JdbcWritableBridge.readString(66, __dbResults);
    this.AGREEMENT_NAAG_ANUM_LEVEL = JdbcWritableBridge.readString(67, __dbResults);
    this.AGREEMENT_NAAG_ANUM = JdbcWritableBridge.readString(68, __dbResults);
    this.AGREEMENT_NAAG_BNUM_LEVEL = JdbcWritableBridge.readString(69, __dbResults);
    this.AGREEMENT_NAAG_BNUM = JdbcWritableBridge.readString(70, __dbResults);
    this.WORLD_VIEW_NAAG_ANUM = JdbcWritableBridge.readString(71, __dbResults);
    this.WORLD_VIEW_ANUM = JdbcWritableBridge.readString(72, __dbResults);
    this.WORLD_VIEW_NAAG_BNUM = JdbcWritableBridge.readString(73, __dbResults);
    this.WORLD_VIEW_BNUM = JdbcWritableBridge.readString(74, __dbResults);
    this.TRAFFIC_BASE_DIRECTION = JdbcWritableBridge.readString(75, __dbResults);
    this.DATA_UNIT = JdbcWritableBridge.readString(76, __dbResults);
    this.DATA_UNIT_2 = JdbcWritableBridge.readString(77, __dbResults);
    this.DATA_UNIT_3 = JdbcWritableBridge.readString(78, __dbResults);
    this.DATA_UNIT_4 = JdbcWritableBridge.readString(79, __dbResults);
    this.DISCRETE_RATING_PARAMETER_1 = JdbcWritableBridge.readString(80, __dbResults);
    this.DISCRETE_RATING_PARAMETER_2 = JdbcWritableBridge.readString(81, __dbResults);
    this.DISCRETE_RATING_PARAMETER_3 = JdbcWritableBridge.readString(82, __dbResults);
    this.REVENUE_SHARE_CURRENCY_1 = JdbcWritableBridge.readString(83, __dbResults);
    this.REVENUE_SHARE_CURRENCY_2 = JdbcWritableBridge.readString(84, __dbResults);
    this.REVENUE_SHARE_CURRENCY_3 = JdbcWritableBridge.readString(85, __dbResults);
    this.ANUM_OPERATOR = JdbcWritableBridge.readString(86, __dbResults);
    this.ANUM_CNP = JdbcWritableBridge.readString(87, __dbResults);
    this.TRAFFIC_NAAG = JdbcWritableBridge.readString(88, __dbResults);
    this.NAAG_ANUM_LEVEL = JdbcWritableBridge.readString(89, __dbResults);
    this.RECON_NAAG_ANUM = JdbcWritableBridge.readString(90, __dbResults);
    this.NETWORK_ADDRESS_AGGR_ANUM = JdbcWritableBridge.readString(91, __dbResults);
    this.NETWORK_TYPE_ANUM = JdbcWritableBridge.readString(92, __dbResults);
    this.BNUM_OPERATOR = JdbcWritableBridge.readString(93, __dbResults);
    this.BNUM_CNP = JdbcWritableBridge.readString(94, __dbResults);
    this.NAAG_BNUM_LEVEL = JdbcWritableBridge.readString(95, __dbResults);
    this.NETWORK_TYPE_BNUM = JdbcWritableBridge.readString(96, __dbResults);
    this.RECON_NAAG_BNUM = JdbcWritableBridge.readString(97, __dbResults);
    this.NETWORK_ADDRESS_AGGR_BNUM = JdbcWritableBridge.readString(98, __dbResults);
    this.DERIVED_PRODUCT_INDICATOR = JdbcWritableBridge.readString(99, __dbResults);
    this.ANUM = JdbcWritableBridge.readString(100, __dbResults);
    this.BNUM = JdbcWritableBridge.readString(101, __dbResults);
    this.RECORD_SEQUENCE_NUMBER = JdbcWritableBridge.readString(102, __dbResults);
    this.USER_DATA = JdbcWritableBridge.readString(103, __dbResults);
    this.USER_DATA_2 = JdbcWritableBridge.readString(104, __dbResults);
    this.USER_DATA_3 = JdbcWritableBridge.readString(105, __dbResults);
    this.CALL_COUNT = JdbcWritableBridge.readBigDecimal(106, __dbResults);
    this.START_CALL_COUNT = JdbcWritableBridge.readBigDecimal(107, __dbResults);
    this.RATE_STEP_CALL_COUNT = JdbcWritableBridge.readBigDecimal(108, __dbResults);
    this.APPORTIONED_CALL_COUNT = JdbcWritableBridge.readBigDecimal(109, __dbResults);
    this.APPORTIONED_DURATION_SECONDS = JdbcWritableBridge.readBigDecimal(110, __dbResults);
    this.ACTUAL_USAGE = JdbcWritableBridge.readBigDecimal(111, __dbResults);
    this.CHARGED_USAGE = JdbcWritableBridge.readBigDecimal(112, __dbResults);
    this.CHARGED_UNITS = JdbcWritableBridge.readString(113, __dbResults);
    this.EVENT_DURATION = JdbcWritableBridge.readBigDecimal(114, __dbResults);
    this.MINIMUM_CHARGE_ADJUSTMENT = JdbcWritableBridge.readBigDecimal(115, __dbResults);
    this.MAXIMUM_CHARGE_ADJUSTMENT = JdbcWritableBridge.readBigDecimal(116, __dbResults);
    this.NETWORK_DURATION = JdbcWritableBridge.readBigDecimal(117, __dbResults);
    this.DATA_VOLUME = JdbcWritableBridge.readBigDecimal(118, __dbResults);
    this.DATA_VOLUME_2 = JdbcWritableBridge.readBigDecimal(119, __dbResults);
    this.DATA_VOLUME_3 = JdbcWritableBridge.readBigDecimal(120, __dbResults);
    this.DATA_VOLUME_4 = JdbcWritableBridge.readBigDecimal(121, __dbResults);
    this.REVENUE_SHARE_AMOUNT_1 = JdbcWritableBridge.readBigDecimal(122, __dbResults);
    this.REVENUE_SHARE_AMOUNT_2 = JdbcWritableBridge.readBigDecimal(123, __dbResults);
    this.REVENUE_SHARE_AMOUNT_3 = JdbcWritableBridge.readBigDecimal(124, __dbResults);
    this.BASE_AMOUNT = JdbcWritableBridge.readBigDecimal(125, __dbResults);
    this.AMOUNT = JdbcWritableBridge.readBigDecimal(126, __dbResults);
    this.DLYS_DETAIL_ID = JdbcWritableBridge.readString(127, __dbResults);
    this.TRAFFIC_PERIOD = JdbcWritableBridge.readString(128, __dbResults);
    this.MESSAGE_DATE = JdbcWritableBridge.readTimestamp(129, __dbResults);
    this.BILLING_DATE = JdbcWritableBridge.readTimestamp(130, __dbResults);
    this.ADJUSTED_DATE = JdbcWritableBridge.readTimestamp(131, __dbResults);
    this.PROCESS_DATE = JdbcWritableBridge.readTimestamp(132, __dbResults);
    this.EVENT_START_DATE = JdbcWritableBridge.readTimestamp(133, __dbResults);
    this.EVENT_START_TIME = JdbcWritableBridge.readString(134, __dbResults);
    this.NETWORK_START_DATE = JdbcWritableBridge.readBigDecimal(135, __dbResults);
    this.NETWORK_START_TIME = JdbcWritableBridge.readBigDecimal(136, __dbResults);
    this.BILLING_START_TIME = JdbcWritableBridge.readBigDecimal(137, __dbResults);
    this.BILLING_END_TIME = JdbcWritableBridge.readBigDecimal(138, __dbResults);
    this.FLAT_RATE_CHARGE = JdbcWritableBridge.readString(139, __dbResults);
    this.RATE_STEP_FLAT_CHARGE = JdbcWritableBridge.readBigDecimal(140, __dbResults);
    this.UNIT_COST_USED = JdbcWritableBridge.readBigDecimal(141, __dbResults);
    this.CHARGE_BUNDLE = JdbcWritableBridge.readBigDecimal(142, __dbResults);
    this.BASE_AMOUNT_FACTOR_USED = JdbcWritableBridge.readBigDecimal(143, __dbResults);
    this.REFUND_FACTOR = JdbcWritableBridge.readBigDecimal(144, __dbResults);
    this.CURRENCY = JdbcWritableBridge.readString(145, __dbResults);
    this.CURRENCY_CONVERSION_RATE = JdbcWritableBridge.readBigDecimal(146, __dbResults);
    this.BASE_UNIT = JdbcWritableBridge.readString(147, __dbResults);
    this.RATE_UNIT = JdbcWritableBridge.readString(148, __dbResults);
    this.ROUNDED_UNIT_ID = JdbcWritableBridge.readBigDecimal(149, __dbResults);
    this.RECORD_TYPE = JdbcWritableBridge.readBigDecimal(150, __dbResults);
    this.LINK_FIELD = JdbcWritableBridge.readBigDecimal(151, __dbResults);
    this.REASON_FOR_CLEARDOWN = JdbcWritableBridge.readBigDecimal(152, __dbResults);
    this.REPAIR_INDICATOR = JdbcWritableBridge.readBigDecimal(153, __dbResults);
    this.RATED_BILLING_PERIOD = JdbcWritableBridge.readBigDecimal(154, __dbResults);
    this.TRAFFIC_MOVEMENT_CTR = JdbcWritableBridge.readBigDecimal(155, __dbResults);
    this.RTE_LOOKUP_STYLE = JdbcWritableBridge.readString(156, __dbResults);
    this.CDR_SOURCE = JdbcWritableBridge.readString(157, __dbResults);
    this.CARRIER_DESTINATION_NAME = JdbcWritableBridge.readString(158, __dbResults);
    this.CARRIER_DESTINATION_ALIAS = JdbcWritableBridge.readString(159, __dbResults);
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
    JdbcWritableBridge.writeBigDecimal(CDR_FILE_NO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CDR_UCI_NO, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CDR_UCI_ELEMENT, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CDR_KEY_ID, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CALL_SCENARIO, 5 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REMUNERATION_INDICATOR, 6 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_BASE_DIRECTION, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(FRANCHISE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRODUCT_GROUP, 9 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVERSE_INDICATOR, 10 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EVENT_DIRECTION, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DERIVED_EVENT_DIRECTION, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_RATING_SCENARIO_TYPE, 13 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLED_PRODUCT, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATING_SCENARIO, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATING_RULE_DEPENDENCY_IND, 16 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ESTIMATE_INDICATOR, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ACCOUNTING_METHOD, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CASH_FLOW, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COMPONENT_DIRECTION, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(STATEMENT_DIRECTION, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_METHOD, 22 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RATING_COMPONENT, 23 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CALL_ATTEMPT_INDICATOR, 24 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PARAMETER_MATRIX, 25 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PARAMETER_MATRIX_LINE, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(STEP_NUMBER, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(RATE_TYPE, 28 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIME_PREMIUM, 29 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_OPERATOR, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATE_OWNER_OPERATOR, 31 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SETTLEMENT_OPERATOR, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATE_NAME, 33 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIER, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIER_GROUP, 35 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIER_TYPE, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OPTIMUM_POI, 37 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ADHOC_SUMMARY_IND, 38 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_SUMMARY_IND, 39 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(COMPLEMENTARY_SUMMARY_IND, 40 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_SUMMARY_IND, 41 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MINIMUM_CHARGE_IND, 42 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CALL_DURATION_BUCKET, 43 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TIME_OF_DAY_BUCKET, 44 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(USER_SUMMARISATION, 45 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_NODE, 46 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_PRODUCT, 47 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_OPERATOR, 48 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_PATH, 49 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_POI, 50 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_NODE, 51 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_PRODUCT, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_OPERATOR, 53 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_PATH, 54 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_POI, 55 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_RATING_SCENARIO_TYPE, 56 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_ROUTE_TYPE, 57 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_ROUTE, 58 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_AGREEMENT_OPERATOR, 59 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_NEGOTIATION_DIR, 60 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_ROUTE_TYPE, 61 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_ROUTE, 62 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_AGREEMENT_OPERATOR, 63 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_NEGOTIATION_DIR, 64 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ROUTE_IDENTIFIER, 65 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REFILE_INDICATOR, 66 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_ANUM_LEVEL, 67 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_ANUM, 68 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_BNUM_LEVEL, 69 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_BNUM, 70 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_NAAG_ANUM, 71 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_ANUM, 72 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_NAAG_BNUM, 73 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_BNUM, 74 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_BASE_DIRECTION, 75 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT, 76 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT_2, 77 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT_3, 78 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT_4, 79 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DISCRETE_RATING_PARAMETER_1, 80 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DISCRETE_RATING_PARAMETER_2, 81 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DISCRETE_RATING_PARAMETER_3, 82 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVENUE_SHARE_CURRENCY_1, 83 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVENUE_SHARE_CURRENCY_2, 84 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVENUE_SHARE_CURRENCY_3, 85 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ANUM_OPERATOR, 86 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANUM_CNP, 87 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_NAAG, 88 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NAAG_ANUM_LEVEL, 89 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RECON_NAAG_ANUM, 90 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_ADDRESS_AGGR_ANUM, 91 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_TYPE_ANUM, 92 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BNUM_OPERATOR, 93 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BNUM_CNP, 94 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NAAG_BNUM_LEVEL, 95 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_TYPE_BNUM, 96 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RECON_NAAG_BNUM, 97 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_ADDRESS_AGGR_BNUM, 98 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DERIVED_PRODUCT_INDICATOR, 99 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ANUM, 100 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BNUM, 101 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RECORD_SEQUENCE_NUMBER, 102 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(USER_DATA, 103 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(USER_DATA_2, 104 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(USER_DATA_3, 105 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CALL_COUNT, 106 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(START_CALL_COUNT, 107 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RATE_STEP_CALL_COUNT, 108 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(APPORTIONED_CALL_COUNT, 109 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(APPORTIONED_DURATION_SECONDS, 110 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ACTUAL_USAGE, 111 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CHARGED_USAGE, 112 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CHARGED_UNITS, 113 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EVENT_DURATION, 114 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(MINIMUM_CHARGE_ADJUSTMENT, 115 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(MAXIMUM_CHARGE_ADJUSTMENT, 116 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NETWORK_DURATION, 117 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME, 118 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME_2, 119 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME_3, 120 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME_4, 121 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REVENUE_SHARE_AMOUNT_1, 122 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REVENUE_SHARE_AMOUNT_2, 123 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REVENUE_SHARE_AMOUNT_3, 124 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BASE_AMOUNT, 125 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(AMOUNT, 126 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(DLYS_DETAIL_ID, 127 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_PERIOD, 128 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeTimestamp(MESSAGE_DATE, 129 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(BILLING_DATE, 130 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(ADJUSTED_DATE, 131 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(PROCESS_DATE, 132 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(EVENT_START_DATE, 133 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(EVENT_START_TIME, 134 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NETWORK_START_DATE, 135 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NETWORK_START_TIME, 136 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BILLING_START_TIME, 137 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BILLING_END_TIME, 138 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FLAT_RATE_CHARGE, 139 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RATE_STEP_FLAT_CHARGE, 140 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(UNIT_COST_USED, 141 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CHARGE_BUNDLE, 142 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BASE_AMOUNT_FACTOR_USED, 143 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REFUND_FACTOR, 144 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CURRENCY, 145 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CURRENCY_CONVERSION_RATE, 146 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(BASE_UNIT, 147 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RATE_UNIT, 148 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ROUNDED_UNIT_ID, 149 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RECORD_TYPE, 150 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(LINK_FIELD, 151 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REASON_FOR_CLEARDOWN, 152 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REPAIR_INDICATOR, 153 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RATED_BILLING_PERIOD, 154 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TRAFFIC_MOVEMENT_CTR, 155 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(RTE_LOOKUP_STYLE, 156 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CDR_SOURCE, 157 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CARRIER_DESTINATION_NAME, 158 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CARRIER_DESTINATION_ALIAS, 159 + __off, 1, __dbStmt);
    return 159;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeBigDecimal(CDR_FILE_NO, 1 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CDR_UCI_NO, 2 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CDR_UCI_ELEMENT, 3 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CDR_KEY_ID, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(CALL_SCENARIO, 5 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REMUNERATION_INDICATOR, 6 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_BASE_DIRECTION, 7 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(FRANCHISE, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(PRODUCT_GROUP, 9 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVERSE_INDICATOR, 10 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(EVENT_DIRECTION, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DERIVED_EVENT_DIRECTION, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_RATING_SCENARIO_TYPE, 13 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLED_PRODUCT, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATING_SCENARIO, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATING_RULE_DEPENDENCY_IND, 16 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ESTIMATE_INDICATOR, 17 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ACCOUNTING_METHOD, 18 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CASH_FLOW, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(COMPONENT_DIRECTION, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(STATEMENT_DIRECTION, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_METHOD, 22 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RATING_COMPONENT, 23 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CALL_ATTEMPT_INDICATOR, 24 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(PARAMETER_MATRIX, 25 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(PARAMETER_MATRIX_LINE, 26 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(STEP_NUMBER, 27 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(RATE_TYPE, 28 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIME_PREMIUM, 29 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_OPERATOR, 30 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATE_OWNER_OPERATOR, 31 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(SETTLEMENT_OPERATOR, 32 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RATE_NAME, 33 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIER, 34 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TIER_GROUP, 35 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TIER_TYPE, 36 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OPTIMUM_POI, 37 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ADHOC_SUMMARY_IND, 38 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_SUMMARY_IND, 39 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(COMPLEMENTARY_SUMMARY_IND, 40 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_SUMMARY_IND, 41 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(MINIMUM_CHARGE_IND, 42 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CALL_DURATION_BUCKET, 43 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TIME_OF_DAY_BUCKET, 44 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(USER_SUMMARISATION, 45 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_NODE, 46 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_PRODUCT, 47 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_OPERATOR, 48 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_PATH, 49 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(INCOMING_POI, 50 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_NODE, 51 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_PRODUCT, 52 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_OPERATOR, 53 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_PATH, 54 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(OUTGOING_POI, 55 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_RATING_SCENARIO_TYPE, 56 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_ROUTE_TYPE, 57 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_ROUTE, 58 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_AGREEMENT_OPERATOR, 59 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_NEGOTIATION_DIR, 60 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_ROUTE_TYPE, 61 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_ROUTE, 62 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_AGREEMENT_OPERATOR, 63 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BILLING_NEGOTIATION_DIR, 64 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ROUTE_IDENTIFIER, 65 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REFILE_INDICATOR, 66 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_ANUM_LEVEL, 67 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_ANUM, 68 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_BNUM_LEVEL, 69 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AGREEMENT_NAAG_BNUM, 70 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_NAAG_ANUM, 71 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_ANUM, 72 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_NAAG_BNUM, 73 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(WORLD_VIEW_BNUM, 74 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_BASE_DIRECTION, 75 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT, 76 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT_2, 77 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT_3, 78 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DATA_UNIT_4, 79 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DISCRETE_RATING_PARAMETER_1, 80 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DISCRETE_RATING_PARAMETER_2, 81 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(DISCRETE_RATING_PARAMETER_3, 82 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVENUE_SHARE_CURRENCY_1, 83 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVENUE_SHARE_CURRENCY_2, 84 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(REVENUE_SHARE_CURRENCY_3, 85 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ANUM_OPERATOR, 86 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(ANUM_CNP, 87 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_NAAG, 88 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NAAG_ANUM_LEVEL, 89 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RECON_NAAG_ANUM, 90 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_ADDRESS_AGGR_ANUM, 91 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_TYPE_ANUM, 92 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(BNUM_OPERATOR, 93 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BNUM_CNP, 94 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(NAAG_BNUM_LEVEL, 95 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_TYPE_BNUM, 96 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RECON_NAAG_BNUM, 97 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(NETWORK_ADDRESS_AGGR_BNUM, 98 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(DERIVED_PRODUCT_INDICATOR, 99 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(ANUM, 100 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(BNUM, 101 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(RECORD_SEQUENCE_NUMBER, 102 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(USER_DATA, 103 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(USER_DATA_2, 104 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(USER_DATA_3, 105 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CALL_COUNT, 106 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(START_CALL_COUNT, 107 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RATE_STEP_CALL_COUNT, 108 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(APPORTIONED_CALL_COUNT, 109 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(APPORTIONED_DURATION_SECONDS, 110 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ACTUAL_USAGE, 111 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CHARGED_USAGE, 112 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CHARGED_UNITS, 113 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(EVENT_DURATION, 114 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(MINIMUM_CHARGE_ADJUSTMENT, 115 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(MAXIMUM_CHARGE_ADJUSTMENT, 116 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NETWORK_DURATION, 117 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME, 118 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME_2, 119 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME_3, 120 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(DATA_VOLUME_4, 121 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REVENUE_SHARE_AMOUNT_1, 122 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REVENUE_SHARE_AMOUNT_2, 123 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REVENUE_SHARE_AMOUNT_3, 124 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BASE_AMOUNT, 125 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(AMOUNT, 126 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(DLYS_DETAIL_ID, 127 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(TRAFFIC_PERIOD, 128 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeTimestamp(MESSAGE_DATE, 129 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(BILLING_DATE, 130 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(ADJUSTED_DATE, 131 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(PROCESS_DATE, 132 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(EVENT_START_DATE, 133 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeString(EVENT_START_TIME, 134 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NETWORK_START_DATE, 135 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(NETWORK_START_TIME, 136 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BILLING_START_TIME, 137 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BILLING_END_TIME, 138 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(FLAT_RATE_CHARGE, 139 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RATE_STEP_FLAT_CHARGE, 140 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(UNIT_COST_USED, 141 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CHARGE_BUNDLE, 142 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(BASE_AMOUNT_FACTOR_USED, 143 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REFUND_FACTOR, 144 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(CURRENCY, 145 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(CURRENCY_CONVERSION_RATE, 146 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(BASE_UNIT, 147 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(RATE_UNIT, 148 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(ROUNDED_UNIT_ID, 149 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RECORD_TYPE, 150 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(LINK_FIELD, 151 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REASON_FOR_CLEARDOWN, 152 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(REPAIR_INDICATOR, 153 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(RATED_BILLING_PERIOD, 154 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(TRAFFIC_MOVEMENT_CTR, 155 + __off, 2, __dbStmt);
    JdbcWritableBridge.writeString(RTE_LOOKUP_STYLE, 156 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CDR_SOURCE, 157 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CARRIER_DESTINATION_NAME, 158 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(CARRIER_DESTINATION_ALIAS, 159 + __off, 1, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.CDR_FILE_NO = null;
    } else {
    this.CDR_FILE_NO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CDR_UCI_NO = null;
    } else {
    this.CDR_UCI_NO = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CDR_UCI_ELEMENT = null;
    } else {
    this.CDR_UCI_ELEMENT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CDR_KEY_ID = null;
    } else {
    this.CDR_KEY_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CALL_SCENARIO = null;
    } else {
    this.CALL_SCENARIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REMUNERATION_INDICATOR = null;
    } else {
    this.REMUNERATION_INDICATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_BASE_DIRECTION = null;
    } else {
    this.BILLING_BASE_DIRECTION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FRANCHISE = null;
    } else {
    this.FRANCHISE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PRODUCT_GROUP = null;
    } else {
    this.PRODUCT_GROUP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVERSE_INDICATOR = null;
    } else {
    this.REVERSE_INDICATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EVENT_DIRECTION = null;
    } else {
    this.EVENT_DIRECTION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DERIVED_EVENT_DIRECTION = null;
    } else {
    this.DERIVED_EVENT_DIRECTION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_RATING_SCENARIO_TYPE = null;
    } else {
    this.BILLING_RATING_SCENARIO_TYPE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLED_PRODUCT = null;
    } else {
    this.BILLED_PRODUCT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATING_SCENARIO = null;
    } else {
    this.RATING_SCENARIO = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATING_RULE_DEPENDENCY_IND = null;
    } else {
    this.RATING_RULE_DEPENDENCY_IND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ESTIMATE_INDICATOR = null;
    } else {
    this.ESTIMATE_INDICATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ACCOUNTING_METHOD = null;
    } else {
    this.ACCOUNTING_METHOD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CASH_FLOW = null;
    } else {
    this.CASH_FLOW = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COMPONENT_DIRECTION = null;
    } else {
    this.COMPONENT_DIRECTION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.STATEMENT_DIRECTION = null;
    } else {
    this.STATEMENT_DIRECTION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_METHOD = null;
    } else {
    this.BILLING_METHOD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATING_COMPONENT = null;
    } else {
    this.RATING_COMPONENT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CALL_ATTEMPT_INDICATOR = null;
    } else {
    this.CALL_ATTEMPT_INDICATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PARAMETER_MATRIX = null;
    } else {
    this.PARAMETER_MATRIX = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.PARAMETER_MATRIX_LINE = null;
    } else {
    this.PARAMETER_MATRIX_LINE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.STEP_NUMBER = null;
    } else {
    this.STEP_NUMBER = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATE_TYPE = null;
    } else {
    this.RATE_TYPE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIME_PREMIUM = null;
    } else {
    this.TIME_PREMIUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_OPERATOR = null;
    } else {
    this.BILLING_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATE_OWNER_OPERATOR = null;
    } else {
    this.RATE_OWNER_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.SETTLEMENT_OPERATOR = null;
    } else {
    this.SETTLEMENT_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATE_NAME = null;
    } else {
    this.RATE_NAME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIER = null;
    } else {
    this.TIER = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIER_GROUP = null;
    } else {
    this.TIER_GROUP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIER_TYPE = null;
    } else {
    this.TIER_TYPE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OPTIMUM_POI = null;
    } else {
    this.OPTIMUM_POI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ADHOC_SUMMARY_IND = null;
    } else {
    this.ADHOC_SUMMARY_IND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_SUMMARY_IND = null;
    } else {
    this.BILLING_SUMMARY_IND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.COMPLEMENTARY_SUMMARY_IND = null;
    } else {
    this.COMPLEMENTARY_SUMMARY_IND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_SUMMARY_IND = null;
    } else {
    this.TRAFFIC_SUMMARY_IND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MINIMUM_CHARGE_IND = null;
    } else {
    this.MINIMUM_CHARGE_IND = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CALL_DURATION_BUCKET = null;
    } else {
    this.CALL_DURATION_BUCKET = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TIME_OF_DAY_BUCKET = null;
    } else {
    this.TIME_OF_DAY_BUCKET = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.USER_SUMMARISATION = null;
    } else {
    this.USER_SUMMARISATION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.INCOMING_NODE = null;
    } else {
    this.INCOMING_NODE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.INCOMING_PRODUCT = null;
    } else {
    this.INCOMING_PRODUCT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.INCOMING_OPERATOR = null;
    } else {
    this.INCOMING_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.INCOMING_PATH = null;
    } else {
    this.INCOMING_PATH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.INCOMING_POI = null;
    } else {
    this.INCOMING_POI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OUTGOING_NODE = null;
    } else {
    this.OUTGOING_NODE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OUTGOING_PRODUCT = null;
    } else {
    this.OUTGOING_PRODUCT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OUTGOING_OPERATOR = null;
    } else {
    this.OUTGOING_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OUTGOING_PATH = null;
    } else {
    this.OUTGOING_PATH = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.OUTGOING_POI = null;
    } else {
    this.OUTGOING_POI = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_RATING_SCENARIO_TYPE = null;
    } else {
    this.TRAFFIC_RATING_SCENARIO_TYPE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_ROUTE_TYPE = null;
    } else {
    this.TRAFFIC_ROUTE_TYPE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_ROUTE = null;
    } else {
    this.TRAFFIC_ROUTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_AGREEMENT_OPERATOR = null;
    } else {
    this.TRAFFIC_AGREEMENT_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_NEGOTIATION_DIR = null;
    } else {
    this.TRAFFIC_NEGOTIATION_DIR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_ROUTE_TYPE = null;
    } else {
    this.BILLING_ROUTE_TYPE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_ROUTE = null;
    } else {
    this.BILLING_ROUTE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_AGREEMENT_OPERATOR = null;
    } else {
    this.BILLING_AGREEMENT_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_NEGOTIATION_DIR = null;
    } else {
    this.BILLING_NEGOTIATION_DIR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ROUTE_IDENTIFIER = null;
    } else {
    this.ROUTE_IDENTIFIER = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REFILE_INDICATOR = null;
    } else {
    this.REFILE_INDICATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AGREEMENT_NAAG_ANUM_LEVEL = null;
    } else {
    this.AGREEMENT_NAAG_ANUM_LEVEL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AGREEMENT_NAAG_ANUM = null;
    } else {
    this.AGREEMENT_NAAG_ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AGREEMENT_NAAG_BNUM_LEVEL = null;
    } else {
    this.AGREEMENT_NAAG_BNUM_LEVEL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AGREEMENT_NAAG_BNUM = null;
    } else {
    this.AGREEMENT_NAAG_BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.WORLD_VIEW_NAAG_ANUM = null;
    } else {
    this.WORLD_VIEW_NAAG_ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.WORLD_VIEW_ANUM = null;
    } else {
    this.WORLD_VIEW_ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.WORLD_VIEW_NAAG_BNUM = null;
    } else {
    this.WORLD_VIEW_NAAG_BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.WORLD_VIEW_BNUM = null;
    } else {
    this.WORLD_VIEW_BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_BASE_DIRECTION = null;
    } else {
    this.TRAFFIC_BASE_DIRECTION = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_UNIT = null;
    } else {
    this.DATA_UNIT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_UNIT_2 = null;
    } else {
    this.DATA_UNIT_2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_UNIT_3 = null;
    } else {
    this.DATA_UNIT_3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_UNIT_4 = null;
    } else {
    this.DATA_UNIT_4 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DISCRETE_RATING_PARAMETER_1 = null;
    } else {
    this.DISCRETE_RATING_PARAMETER_1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DISCRETE_RATING_PARAMETER_2 = null;
    } else {
    this.DISCRETE_RATING_PARAMETER_2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DISCRETE_RATING_PARAMETER_3 = null;
    } else {
    this.DISCRETE_RATING_PARAMETER_3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVENUE_SHARE_CURRENCY_1 = null;
    } else {
    this.REVENUE_SHARE_CURRENCY_1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVENUE_SHARE_CURRENCY_2 = null;
    } else {
    this.REVENUE_SHARE_CURRENCY_2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVENUE_SHARE_CURRENCY_3 = null;
    } else {
    this.REVENUE_SHARE_CURRENCY_3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ANUM_OPERATOR = null;
    } else {
    this.ANUM_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ANUM_CNP = null;
    } else {
    this.ANUM_CNP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_NAAG = null;
    } else {
    this.TRAFFIC_NAAG = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NAAG_ANUM_LEVEL = null;
    } else {
    this.NAAG_ANUM_LEVEL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RECON_NAAG_ANUM = null;
    } else {
    this.RECON_NAAG_ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_ADDRESS_AGGR_ANUM = null;
    } else {
    this.NETWORK_ADDRESS_AGGR_ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_TYPE_ANUM = null;
    } else {
    this.NETWORK_TYPE_ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BNUM_OPERATOR = null;
    } else {
    this.BNUM_OPERATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BNUM_CNP = null;
    } else {
    this.BNUM_CNP = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NAAG_BNUM_LEVEL = null;
    } else {
    this.NAAG_BNUM_LEVEL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_TYPE_BNUM = null;
    } else {
    this.NETWORK_TYPE_BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RECON_NAAG_BNUM = null;
    } else {
    this.RECON_NAAG_BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_ADDRESS_AGGR_BNUM = null;
    } else {
    this.NETWORK_ADDRESS_AGGR_BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DERIVED_PRODUCT_INDICATOR = null;
    } else {
    this.DERIVED_PRODUCT_INDICATOR = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ANUM = null;
    } else {
    this.ANUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BNUM = null;
    } else {
    this.BNUM = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RECORD_SEQUENCE_NUMBER = null;
    } else {
    this.RECORD_SEQUENCE_NUMBER = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.USER_DATA = null;
    } else {
    this.USER_DATA = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.USER_DATA_2 = null;
    } else {
    this.USER_DATA_2 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.USER_DATA_3 = null;
    } else {
    this.USER_DATA_3 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CALL_COUNT = null;
    } else {
    this.CALL_COUNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.START_CALL_COUNT = null;
    } else {
    this.START_CALL_COUNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATE_STEP_CALL_COUNT = null;
    } else {
    this.RATE_STEP_CALL_COUNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.APPORTIONED_CALL_COUNT = null;
    } else {
    this.APPORTIONED_CALL_COUNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.APPORTIONED_DURATION_SECONDS = null;
    } else {
    this.APPORTIONED_DURATION_SECONDS = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ACTUAL_USAGE = null;
    } else {
    this.ACTUAL_USAGE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHARGED_USAGE = null;
    } else {
    this.CHARGED_USAGE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHARGED_UNITS = null;
    } else {
    this.CHARGED_UNITS = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EVENT_DURATION = null;
    } else {
    this.EVENT_DURATION = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MINIMUM_CHARGE_ADJUSTMENT = null;
    } else {
    this.MINIMUM_CHARGE_ADJUSTMENT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MAXIMUM_CHARGE_ADJUSTMENT = null;
    } else {
    this.MAXIMUM_CHARGE_ADJUSTMENT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_DURATION = null;
    } else {
    this.NETWORK_DURATION = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_VOLUME = null;
    } else {
    this.DATA_VOLUME = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_VOLUME_2 = null;
    } else {
    this.DATA_VOLUME_2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_VOLUME_3 = null;
    } else {
    this.DATA_VOLUME_3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DATA_VOLUME_4 = null;
    } else {
    this.DATA_VOLUME_4 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVENUE_SHARE_AMOUNT_1 = null;
    } else {
    this.REVENUE_SHARE_AMOUNT_1 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVENUE_SHARE_AMOUNT_2 = null;
    } else {
    this.REVENUE_SHARE_AMOUNT_2 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REVENUE_SHARE_AMOUNT_3 = null;
    } else {
    this.REVENUE_SHARE_AMOUNT_3 = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BASE_AMOUNT = null;
    } else {
    this.BASE_AMOUNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AMOUNT = null;
    } else {
    this.AMOUNT = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.DLYS_DETAIL_ID = null;
    } else {
    this.DLYS_DETAIL_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_PERIOD = null;
    } else {
    this.TRAFFIC_PERIOD = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MESSAGE_DATE = null;
    } else {
    this.MESSAGE_DATE = new Timestamp(__dataIn.readLong());
    this.MESSAGE_DATE.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_DATE = null;
    } else {
    this.BILLING_DATE = new Timestamp(__dataIn.readLong());
    this.BILLING_DATE.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.ADJUSTED_DATE = null;
    } else {
    this.ADJUSTED_DATE = new Timestamp(__dataIn.readLong());
    this.ADJUSTED_DATE.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.PROCESS_DATE = null;
    } else {
    this.PROCESS_DATE = new Timestamp(__dataIn.readLong());
    this.PROCESS_DATE.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.EVENT_START_DATE = null;
    } else {
    this.EVENT_START_DATE = new Timestamp(__dataIn.readLong());
    this.EVENT_START_DATE.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.EVENT_START_TIME = null;
    } else {
    this.EVENT_START_TIME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_START_DATE = null;
    } else {
    this.NETWORK_START_DATE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.NETWORK_START_TIME = null;
    } else {
    this.NETWORK_START_TIME = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_START_TIME = null;
    } else {
    this.BILLING_START_TIME = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BILLING_END_TIME = null;
    } else {
    this.BILLING_END_TIME = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FLAT_RATE_CHARGE = null;
    } else {
    this.FLAT_RATE_CHARGE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATE_STEP_FLAT_CHARGE = null;
    } else {
    this.RATE_STEP_FLAT_CHARGE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.UNIT_COST_USED = null;
    } else {
    this.UNIT_COST_USED = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CHARGE_BUNDLE = null;
    } else {
    this.CHARGE_BUNDLE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BASE_AMOUNT_FACTOR_USED = null;
    } else {
    this.BASE_AMOUNT_FACTOR_USED = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REFUND_FACTOR = null;
    } else {
    this.REFUND_FACTOR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CURRENCY = null;
    } else {
    this.CURRENCY = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CURRENCY_CONVERSION_RATE = null;
    } else {
    this.CURRENCY_CONVERSION_RATE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.BASE_UNIT = null;
    } else {
    this.BASE_UNIT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATE_UNIT = null;
    } else {
    this.RATE_UNIT = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.ROUNDED_UNIT_ID = null;
    } else {
    this.ROUNDED_UNIT_ID = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RECORD_TYPE = null;
    } else {
    this.RECORD_TYPE = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.LINK_FIELD = null;
    } else {
    this.LINK_FIELD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REASON_FOR_CLEARDOWN = null;
    } else {
    this.REASON_FOR_CLEARDOWN = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.REPAIR_INDICATOR = null;
    } else {
    this.REPAIR_INDICATOR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RATED_BILLING_PERIOD = null;
    } else {
    this.RATED_BILLING_PERIOD = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TRAFFIC_MOVEMENT_CTR = null;
    } else {
    this.TRAFFIC_MOVEMENT_CTR = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.RTE_LOOKUP_STYLE = null;
    } else {
    this.RTE_LOOKUP_STYLE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CDR_SOURCE = null;
    } else {
    this.CDR_SOURCE = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CARRIER_DESTINATION_NAME = null;
    } else {
    this.CARRIER_DESTINATION_NAME = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.CARRIER_DESTINATION_ALIAS = null;
    } else {
    this.CARRIER_DESTINATION_ALIAS = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.CDR_FILE_NO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CDR_FILE_NO, __dataOut);
    }
    if (null == this.CDR_UCI_NO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CDR_UCI_NO, __dataOut);
    }
    if (null == this.CDR_UCI_ELEMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CDR_UCI_ELEMENT, __dataOut);
    }
    if (null == this.CDR_KEY_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CDR_KEY_ID);
    }
    if (null == this.CALL_SCENARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CALL_SCENARIO);
    }
    if (null == this.REMUNERATION_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REMUNERATION_INDICATOR);
    }
    if (null == this.BILLING_BASE_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_BASE_DIRECTION);
    }
    if (null == this.FRANCHISE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FRANCHISE);
    }
    if (null == this.PRODUCT_GROUP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRODUCT_GROUP);
    }
    if (null == this.REVERSE_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVERSE_INDICATOR);
    }
    if (null == this.EVENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EVENT_DIRECTION);
    }
    if (null == this.DERIVED_EVENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DERIVED_EVENT_DIRECTION);
    }
    if (null == this.BILLING_RATING_SCENARIO_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_RATING_SCENARIO_TYPE);
    }
    if (null == this.BILLED_PRODUCT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLED_PRODUCT);
    }
    if (null == this.RATING_SCENARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATING_SCENARIO);
    }
    if (null == this.RATING_RULE_DEPENDENCY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATING_RULE_DEPENDENCY_IND);
    }
    if (null == this.ESTIMATE_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ESTIMATE_INDICATOR);
    }
    if (null == this.ACCOUNTING_METHOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ACCOUNTING_METHOD);
    }
    if (null == this.CASH_FLOW) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CASH_FLOW);
    }
    if (null == this.COMPONENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COMPONENT_DIRECTION);
    }
    if (null == this.STATEMENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, STATEMENT_DIRECTION);
    }
    if (null == this.BILLING_METHOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_METHOD);
    }
    if (null == this.RATING_COMPONENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATING_COMPONENT);
    }
    if (null == this.CALL_ATTEMPT_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CALL_ATTEMPT_INDICATOR);
    }
    if (null == this.PARAMETER_MATRIX) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PARAMETER_MATRIX);
    }
    if (null == this.PARAMETER_MATRIX_LINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PARAMETER_MATRIX_LINE, __dataOut);
    }
    if (null == this.STEP_NUMBER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.STEP_NUMBER, __dataOut);
    }
    if (null == this.RATE_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_TYPE);
    }
    if (null == this.TIME_PREMIUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIME_PREMIUM);
    }
    if (null == this.BILLING_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_OPERATOR);
    }
    if (null == this.RATE_OWNER_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_OWNER_OPERATOR);
    }
    if (null == this.SETTLEMENT_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SETTLEMENT_OPERATOR);
    }
    if (null == this.RATE_NAME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_NAME);
    }
    if (null == this.TIER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIER);
    }
    if (null == this.TIER_GROUP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIER_GROUP);
    }
    if (null == this.TIER_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIER_TYPE);
    }
    if (null == this.OPTIMUM_POI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OPTIMUM_POI);
    }
    if (null == this.ADHOC_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ADHOC_SUMMARY_IND);
    }
    if (null == this.BILLING_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_SUMMARY_IND);
    }
    if (null == this.COMPLEMENTARY_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COMPLEMENTARY_SUMMARY_IND);
    }
    if (null == this.TRAFFIC_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_SUMMARY_IND);
    }
    if (null == this.MINIMUM_CHARGE_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MINIMUM_CHARGE_IND);
    }
    if (null == this.CALL_DURATION_BUCKET) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CALL_DURATION_BUCKET, __dataOut);
    }
    if (null == this.TIME_OF_DAY_BUCKET) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.TIME_OF_DAY_BUCKET, __dataOut);
    }
    if (null == this.USER_SUMMARISATION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_SUMMARISATION);
    }
    if (null == this.INCOMING_NODE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_NODE);
    }
    if (null == this.INCOMING_PRODUCT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_PRODUCT);
    }
    if (null == this.INCOMING_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_OPERATOR);
    }
    if (null == this.INCOMING_PATH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_PATH);
    }
    if (null == this.INCOMING_POI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_POI);
    }
    if (null == this.OUTGOING_NODE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_NODE);
    }
    if (null == this.OUTGOING_PRODUCT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_PRODUCT);
    }
    if (null == this.OUTGOING_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_OPERATOR);
    }
    if (null == this.OUTGOING_PATH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_PATH);
    }
    if (null == this.OUTGOING_POI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_POI);
    }
    if (null == this.TRAFFIC_RATING_SCENARIO_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_RATING_SCENARIO_TYPE);
    }
    if (null == this.TRAFFIC_ROUTE_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_ROUTE_TYPE);
    }
    if (null == this.TRAFFIC_ROUTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_ROUTE);
    }
    if (null == this.TRAFFIC_AGREEMENT_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_AGREEMENT_OPERATOR);
    }
    if (null == this.TRAFFIC_NEGOTIATION_DIR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_NEGOTIATION_DIR);
    }
    if (null == this.BILLING_ROUTE_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_ROUTE_TYPE);
    }
    if (null == this.BILLING_ROUTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_ROUTE);
    }
    if (null == this.BILLING_AGREEMENT_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_AGREEMENT_OPERATOR);
    }
    if (null == this.BILLING_NEGOTIATION_DIR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_NEGOTIATION_DIR);
    }
    if (null == this.ROUTE_IDENTIFIER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ROUTE_IDENTIFIER);
    }
    if (null == this.REFILE_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REFILE_INDICATOR);
    }
    if (null == this.AGREEMENT_NAAG_ANUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_ANUM_LEVEL);
    }
    if (null == this.AGREEMENT_NAAG_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_ANUM);
    }
    if (null == this.AGREEMENT_NAAG_BNUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_BNUM_LEVEL);
    }
    if (null == this.AGREEMENT_NAAG_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_BNUM);
    }
    if (null == this.WORLD_VIEW_NAAG_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_NAAG_ANUM);
    }
    if (null == this.WORLD_VIEW_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_ANUM);
    }
    if (null == this.WORLD_VIEW_NAAG_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_NAAG_BNUM);
    }
    if (null == this.WORLD_VIEW_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_BNUM);
    }
    if (null == this.TRAFFIC_BASE_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_BASE_DIRECTION);
    }
    if (null == this.DATA_UNIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT);
    }
    if (null == this.DATA_UNIT_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT_2);
    }
    if (null == this.DATA_UNIT_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT_3);
    }
    if (null == this.DATA_UNIT_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT_4);
    }
    if (null == this.DISCRETE_RATING_PARAMETER_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DISCRETE_RATING_PARAMETER_1);
    }
    if (null == this.DISCRETE_RATING_PARAMETER_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DISCRETE_RATING_PARAMETER_2);
    }
    if (null == this.DISCRETE_RATING_PARAMETER_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DISCRETE_RATING_PARAMETER_3);
    }
    if (null == this.REVENUE_SHARE_CURRENCY_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVENUE_SHARE_CURRENCY_1);
    }
    if (null == this.REVENUE_SHARE_CURRENCY_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVENUE_SHARE_CURRENCY_2);
    }
    if (null == this.REVENUE_SHARE_CURRENCY_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVENUE_SHARE_CURRENCY_3);
    }
    if (null == this.ANUM_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANUM_OPERATOR);
    }
    if (null == this.ANUM_CNP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANUM_CNP);
    }
    if (null == this.TRAFFIC_NAAG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_NAAG);
    }
    if (null == this.NAAG_ANUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NAAG_ANUM_LEVEL);
    }
    if (null == this.RECON_NAAG_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECON_NAAG_ANUM);
    }
    if (null == this.NETWORK_ADDRESS_AGGR_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_ADDRESS_AGGR_ANUM);
    }
    if (null == this.NETWORK_TYPE_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_TYPE_ANUM);
    }
    if (null == this.BNUM_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNUM_OPERATOR);
    }
    if (null == this.BNUM_CNP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNUM_CNP);
    }
    if (null == this.NAAG_BNUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NAAG_BNUM_LEVEL);
    }
    if (null == this.NETWORK_TYPE_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_TYPE_BNUM);
    }
    if (null == this.RECON_NAAG_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECON_NAAG_BNUM);
    }
    if (null == this.NETWORK_ADDRESS_AGGR_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_ADDRESS_AGGR_BNUM);
    }
    if (null == this.DERIVED_PRODUCT_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DERIVED_PRODUCT_INDICATOR);
    }
    if (null == this.ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANUM);
    }
    if (null == this.BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNUM);
    }
    if (null == this.RECORD_SEQUENCE_NUMBER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECORD_SEQUENCE_NUMBER);
    }
    if (null == this.USER_DATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_DATA);
    }
    if (null == this.USER_DATA_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_DATA_2);
    }
    if (null == this.USER_DATA_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_DATA_3);
    }
    if (null == this.CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CALL_COUNT, __dataOut);
    }
    if (null == this.START_CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.START_CALL_COUNT, __dataOut);
    }
    if (null == this.RATE_STEP_CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RATE_STEP_CALL_COUNT, __dataOut);
    }
    if (null == this.APPORTIONED_CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.APPORTIONED_CALL_COUNT, __dataOut);
    }
    if (null == this.APPORTIONED_DURATION_SECONDS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.APPORTIONED_DURATION_SECONDS, __dataOut);
    }
    if (null == this.ACTUAL_USAGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ACTUAL_USAGE, __dataOut);
    }
    if (null == this.CHARGED_USAGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CHARGED_USAGE, __dataOut);
    }
    if (null == this.CHARGED_UNITS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHARGED_UNITS);
    }
    if (null == this.EVENT_DURATION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EVENT_DURATION, __dataOut);
    }
    if (null == this.MINIMUM_CHARGE_ADJUSTMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.MINIMUM_CHARGE_ADJUSTMENT, __dataOut);
    }
    if (null == this.MAXIMUM_CHARGE_ADJUSTMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.MAXIMUM_CHARGE_ADJUSTMENT, __dataOut);
    }
    if (null == this.NETWORK_DURATION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NETWORK_DURATION, __dataOut);
    }
    if (null == this.DATA_VOLUME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME, __dataOut);
    }
    if (null == this.DATA_VOLUME_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME_2, __dataOut);
    }
    if (null == this.DATA_VOLUME_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME_3, __dataOut);
    }
    if (null == this.DATA_VOLUME_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME_4, __dataOut);
    }
    if (null == this.REVENUE_SHARE_AMOUNT_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REVENUE_SHARE_AMOUNT_1, __dataOut);
    }
    if (null == this.REVENUE_SHARE_AMOUNT_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REVENUE_SHARE_AMOUNT_2, __dataOut);
    }
    if (null == this.REVENUE_SHARE_AMOUNT_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REVENUE_SHARE_AMOUNT_3, __dataOut);
    }
    if (null == this.BASE_AMOUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BASE_AMOUNT, __dataOut);
    }
    if (null == this.AMOUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.AMOUNT, __dataOut);
    }
    if (null == this.DLYS_DETAIL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DLYS_DETAIL_ID);
    }
    if (null == this.TRAFFIC_PERIOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_PERIOD);
    }
    if (null == this.MESSAGE_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.MESSAGE_DATE.getTime());
    __dataOut.writeInt(this.MESSAGE_DATE.getNanos());
    }
    if (null == this.BILLING_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.BILLING_DATE.getTime());
    __dataOut.writeInt(this.BILLING_DATE.getNanos());
    }
    if (null == this.ADJUSTED_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ADJUSTED_DATE.getTime());
    __dataOut.writeInt(this.ADJUSTED_DATE.getNanos());
    }
    if (null == this.PROCESS_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.PROCESS_DATE.getTime());
    __dataOut.writeInt(this.PROCESS_DATE.getNanos());
    }
    if (null == this.EVENT_START_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.EVENT_START_DATE.getTime());
    __dataOut.writeInt(this.EVENT_START_DATE.getNanos());
    }
    if (null == this.EVENT_START_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EVENT_START_TIME);
    }
    if (null == this.NETWORK_START_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NETWORK_START_DATE, __dataOut);
    }
    if (null == this.NETWORK_START_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NETWORK_START_TIME, __dataOut);
    }
    if (null == this.BILLING_START_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BILLING_START_TIME, __dataOut);
    }
    if (null == this.BILLING_END_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BILLING_END_TIME, __dataOut);
    }
    if (null == this.FLAT_RATE_CHARGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FLAT_RATE_CHARGE);
    }
    if (null == this.RATE_STEP_FLAT_CHARGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RATE_STEP_FLAT_CHARGE, __dataOut);
    }
    if (null == this.UNIT_COST_USED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.UNIT_COST_USED, __dataOut);
    }
    if (null == this.CHARGE_BUNDLE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CHARGE_BUNDLE, __dataOut);
    }
    if (null == this.BASE_AMOUNT_FACTOR_USED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BASE_AMOUNT_FACTOR_USED, __dataOut);
    }
    if (null == this.REFUND_FACTOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REFUND_FACTOR, __dataOut);
    }
    if (null == this.CURRENCY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CURRENCY);
    }
    if (null == this.CURRENCY_CONVERSION_RATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CURRENCY_CONVERSION_RATE, __dataOut);
    }
    if (null == this.BASE_UNIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BASE_UNIT);
    }
    if (null == this.RATE_UNIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_UNIT);
    }
    if (null == this.ROUNDED_UNIT_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ROUNDED_UNIT_ID, __dataOut);
    }
    if (null == this.RECORD_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RECORD_TYPE, __dataOut);
    }
    if (null == this.LINK_FIELD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.LINK_FIELD, __dataOut);
    }
    if (null == this.REASON_FOR_CLEARDOWN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REASON_FOR_CLEARDOWN, __dataOut);
    }
    if (null == this.REPAIR_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REPAIR_INDICATOR, __dataOut);
    }
    if (null == this.RATED_BILLING_PERIOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RATED_BILLING_PERIOD, __dataOut);
    }
    if (null == this.TRAFFIC_MOVEMENT_CTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.TRAFFIC_MOVEMENT_CTR, __dataOut);
    }
    if (null == this.RTE_LOOKUP_STYLE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RTE_LOOKUP_STYLE);
    }
    if (null == this.CDR_SOURCE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CDR_SOURCE);
    }
    if (null == this.CARRIER_DESTINATION_NAME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CARRIER_DESTINATION_NAME);
    }
    if (null == this.CARRIER_DESTINATION_ALIAS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CARRIER_DESTINATION_ALIAS);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.CDR_FILE_NO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CDR_FILE_NO, __dataOut);
    }
    if (null == this.CDR_UCI_NO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CDR_UCI_NO, __dataOut);
    }
    if (null == this.CDR_UCI_ELEMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CDR_UCI_ELEMENT, __dataOut);
    }
    if (null == this.CDR_KEY_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CDR_KEY_ID);
    }
    if (null == this.CALL_SCENARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CALL_SCENARIO);
    }
    if (null == this.REMUNERATION_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REMUNERATION_INDICATOR);
    }
    if (null == this.BILLING_BASE_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_BASE_DIRECTION);
    }
    if (null == this.FRANCHISE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FRANCHISE);
    }
    if (null == this.PRODUCT_GROUP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PRODUCT_GROUP);
    }
    if (null == this.REVERSE_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVERSE_INDICATOR);
    }
    if (null == this.EVENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EVENT_DIRECTION);
    }
    if (null == this.DERIVED_EVENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DERIVED_EVENT_DIRECTION);
    }
    if (null == this.BILLING_RATING_SCENARIO_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_RATING_SCENARIO_TYPE);
    }
    if (null == this.BILLED_PRODUCT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLED_PRODUCT);
    }
    if (null == this.RATING_SCENARIO) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATING_SCENARIO);
    }
    if (null == this.RATING_RULE_DEPENDENCY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATING_RULE_DEPENDENCY_IND);
    }
    if (null == this.ESTIMATE_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ESTIMATE_INDICATOR);
    }
    if (null == this.ACCOUNTING_METHOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ACCOUNTING_METHOD);
    }
    if (null == this.CASH_FLOW) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CASH_FLOW);
    }
    if (null == this.COMPONENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COMPONENT_DIRECTION);
    }
    if (null == this.STATEMENT_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, STATEMENT_DIRECTION);
    }
    if (null == this.BILLING_METHOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_METHOD);
    }
    if (null == this.RATING_COMPONENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATING_COMPONENT);
    }
    if (null == this.CALL_ATTEMPT_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CALL_ATTEMPT_INDICATOR);
    }
    if (null == this.PARAMETER_MATRIX) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, PARAMETER_MATRIX);
    }
    if (null == this.PARAMETER_MATRIX_LINE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.PARAMETER_MATRIX_LINE, __dataOut);
    }
    if (null == this.STEP_NUMBER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.STEP_NUMBER, __dataOut);
    }
    if (null == this.RATE_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_TYPE);
    }
    if (null == this.TIME_PREMIUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIME_PREMIUM);
    }
    if (null == this.BILLING_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_OPERATOR);
    }
    if (null == this.RATE_OWNER_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_OWNER_OPERATOR);
    }
    if (null == this.SETTLEMENT_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SETTLEMENT_OPERATOR);
    }
    if (null == this.RATE_NAME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_NAME);
    }
    if (null == this.TIER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIER);
    }
    if (null == this.TIER_GROUP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIER_GROUP);
    }
    if (null == this.TIER_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TIER_TYPE);
    }
    if (null == this.OPTIMUM_POI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OPTIMUM_POI);
    }
    if (null == this.ADHOC_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ADHOC_SUMMARY_IND);
    }
    if (null == this.BILLING_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_SUMMARY_IND);
    }
    if (null == this.COMPLEMENTARY_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, COMPLEMENTARY_SUMMARY_IND);
    }
    if (null == this.TRAFFIC_SUMMARY_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_SUMMARY_IND);
    }
    if (null == this.MINIMUM_CHARGE_IND) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, MINIMUM_CHARGE_IND);
    }
    if (null == this.CALL_DURATION_BUCKET) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CALL_DURATION_BUCKET, __dataOut);
    }
    if (null == this.TIME_OF_DAY_BUCKET) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.TIME_OF_DAY_BUCKET, __dataOut);
    }
    if (null == this.USER_SUMMARISATION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_SUMMARISATION);
    }
    if (null == this.INCOMING_NODE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_NODE);
    }
    if (null == this.INCOMING_PRODUCT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_PRODUCT);
    }
    if (null == this.INCOMING_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_OPERATOR);
    }
    if (null == this.INCOMING_PATH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_PATH);
    }
    if (null == this.INCOMING_POI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, INCOMING_POI);
    }
    if (null == this.OUTGOING_NODE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_NODE);
    }
    if (null == this.OUTGOING_PRODUCT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_PRODUCT);
    }
    if (null == this.OUTGOING_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_OPERATOR);
    }
    if (null == this.OUTGOING_PATH) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_PATH);
    }
    if (null == this.OUTGOING_POI) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, OUTGOING_POI);
    }
    if (null == this.TRAFFIC_RATING_SCENARIO_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_RATING_SCENARIO_TYPE);
    }
    if (null == this.TRAFFIC_ROUTE_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_ROUTE_TYPE);
    }
    if (null == this.TRAFFIC_ROUTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_ROUTE);
    }
    if (null == this.TRAFFIC_AGREEMENT_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_AGREEMENT_OPERATOR);
    }
    if (null == this.TRAFFIC_NEGOTIATION_DIR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_NEGOTIATION_DIR);
    }
    if (null == this.BILLING_ROUTE_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_ROUTE_TYPE);
    }
    if (null == this.BILLING_ROUTE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_ROUTE);
    }
    if (null == this.BILLING_AGREEMENT_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_AGREEMENT_OPERATOR);
    }
    if (null == this.BILLING_NEGOTIATION_DIR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BILLING_NEGOTIATION_DIR);
    }
    if (null == this.ROUTE_IDENTIFIER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ROUTE_IDENTIFIER);
    }
    if (null == this.REFILE_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REFILE_INDICATOR);
    }
    if (null == this.AGREEMENT_NAAG_ANUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_ANUM_LEVEL);
    }
    if (null == this.AGREEMENT_NAAG_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_ANUM);
    }
    if (null == this.AGREEMENT_NAAG_BNUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_BNUM_LEVEL);
    }
    if (null == this.AGREEMENT_NAAG_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AGREEMENT_NAAG_BNUM);
    }
    if (null == this.WORLD_VIEW_NAAG_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_NAAG_ANUM);
    }
    if (null == this.WORLD_VIEW_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_ANUM);
    }
    if (null == this.WORLD_VIEW_NAAG_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_NAAG_BNUM);
    }
    if (null == this.WORLD_VIEW_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, WORLD_VIEW_BNUM);
    }
    if (null == this.TRAFFIC_BASE_DIRECTION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_BASE_DIRECTION);
    }
    if (null == this.DATA_UNIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT);
    }
    if (null == this.DATA_UNIT_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT_2);
    }
    if (null == this.DATA_UNIT_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT_3);
    }
    if (null == this.DATA_UNIT_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DATA_UNIT_4);
    }
    if (null == this.DISCRETE_RATING_PARAMETER_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DISCRETE_RATING_PARAMETER_1);
    }
    if (null == this.DISCRETE_RATING_PARAMETER_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DISCRETE_RATING_PARAMETER_2);
    }
    if (null == this.DISCRETE_RATING_PARAMETER_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DISCRETE_RATING_PARAMETER_3);
    }
    if (null == this.REVENUE_SHARE_CURRENCY_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVENUE_SHARE_CURRENCY_1);
    }
    if (null == this.REVENUE_SHARE_CURRENCY_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVENUE_SHARE_CURRENCY_2);
    }
    if (null == this.REVENUE_SHARE_CURRENCY_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, REVENUE_SHARE_CURRENCY_3);
    }
    if (null == this.ANUM_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANUM_OPERATOR);
    }
    if (null == this.ANUM_CNP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANUM_CNP);
    }
    if (null == this.TRAFFIC_NAAG) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_NAAG);
    }
    if (null == this.NAAG_ANUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NAAG_ANUM_LEVEL);
    }
    if (null == this.RECON_NAAG_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECON_NAAG_ANUM);
    }
    if (null == this.NETWORK_ADDRESS_AGGR_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_ADDRESS_AGGR_ANUM);
    }
    if (null == this.NETWORK_TYPE_ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_TYPE_ANUM);
    }
    if (null == this.BNUM_OPERATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNUM_OPERATOR);
    }
    if (null == this.BNUM_CNP) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNUM_CNP);
    }
    if (null == this.NAAG_BNUM_LEVEL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NAAG_BNUM_LEVEL);
    }
    if (null == this.NETWORK_TYPE_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_TYPE_BNUM);
    }
    if (null == this.RECON_NAAG_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECON_NAAG_BNUM);
    }
    if (null == this.NETWORK_ADDRESS_AGGR_BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, NETWORK_ADDRESS_AGGR_BNUM);
    }
    if (null == this.DERIVED_PRODUCT_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DERIVED_PRODUCT_INDICATOR);
    }
    if (null == this.ANUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, ANUM);
    }
    if (null == this.BNUM) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BNUM);
    }
    if (null == this.RECORD_SEQUENCE_NUMBER) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RECORD_SEQUENCE_NUMBER);
    }
    if (null == this.USER_DATA) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_DATA);
    }
    if (null == this.USER_DATA_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_DATA_2);
    }
    if (null == this.USER_DATA_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, USER_DATA_3);
    }
    if (null == this.CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CALL_COUNT, __dataOut);
    }
    if (null == this.START_CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.START_CALL_COUNT, __dataOut);
    }
    if (null == this.RATE_STEP_CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RATE_STEP_CALL_COUNT, __dataOut);
    }
    if (null == this.APPORTIONED_CALL_COUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.APPORTIONED_CALL_COUNT, __dataOut);
    }
    if (null == this.APPORTIONED_DURATION_SECONDS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.APPORTIONED_DURATION_SECONDS, __dataOut);
    }
    if (null == this.ACTUAL_USAGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ACTUAL_USAGE, __dataOut);
    }
    if (null == this.CHARGED_USAGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CHARGED_USAGE, __dataOut);
    }
    if (null == this.CHARGED_UNITS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CHARGED_UNITS);
    }
    if (null == this.EVENT_DURATION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.EVENT_DURATION, __dataOut);
    }
    if (null == this.MINIMUM_CHARGE_ADJUSTMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.MINIMUM_CHARGE_ADJUSTMENT, __dataOut);
    }
    if (null == this.MAXIMUM_CHARGE_ADJUSTMENT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.MAXIMUM_CHARGE_ADJUSTMENT, __dataOut);
    }
    if (null == this.NETWORK_DURATION) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NETWORK_DURATION, __dataOut);
    }
    if (null == this.DATA_VOLUME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME, __dataOut);
    }
    if (null == this.DATA_VOLUME_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME_2, __dataOut);
    }
    if (null == this.DATA_VOLUME_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME_3, __dataOut);
    }
    if (null == this.DATA_VOLUME_4) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.DATA_VOLUME_4, __dataOut);
    }
    if (null == this.REVENUE_SHARE_AMOUNT_1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REVENUE_SHARE_AMOUNT_1, __dataOut);
    }
    if (null == this.REVENUE_SHARE_AMOUNT_2) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REVENUE_SHARE_AMOUNT_2, __dataOut);
    }
    if (null == this.REVENUE_SHARE_AMOUNT_3) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REVENUE_SHARE_AMOUNT_3, __dataOut);
    }
    if (null == this.BASE_AMOUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BASE_AMOUNT, __dataOut);
    }
    if (null == this.AMOUNT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.AMOUNT, __dataOut);
    }
    if (null == this.DLYS_DETAIL_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, DLYS_DETAIL_ID);
    }
    if (null == this.TRAFFIC_PERIOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TRAFFIC_PERIOD);
    }
    if (null == this.MESSAGE_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.MESSAGE_DATE.getTime());
    __dataOut.writeInt(this.MESSAGE_DATE.getNanos());
    }
    if (null == this.BILLING_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.BILLING_DATE.getTime());
    __dataOut.writeInt(this.BILLING_DATE.getNanos());
    }
    if (null == this.ADJUSTED_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ADJUSTED_DATE.getTime());
    __dataOut.writeInt(this.ADJUSTED_DATE.getNanos());
    }
    if (null == this.PROCESS_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.PROCESS_DATE.getTime());
    __dataOut.writeInt(this.PROCESS_DATE.getNanos());
    }
    if (null == this.EVENT_START_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.EVENT_START_DATE.getTime());
    __dataOut.writeInt(this.EVENT_START_DATE.getNanos());
    }
    if (null == this.EVENT_START_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EVENT_START_TIME);
    }
    if (null == this.NETWORK_START_DATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NETWORK_START_DATE, __dataOut);
    }
    if (null == this.NETWORK_START_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.NETWORK_START_TIME, __dataOut);
    }
    if (null == this.BILLING_START_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BILLING_START_TIME, __dataOut);
    }
    if (null == this.BILLING_END_TIME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BILLING_END_TIME, __dataOut);
    }
    if (null == this.FLAT_RATE_CHARGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, FLAT_RATE_CHARGE);
    }
    if (null == this.RATE_STEP_FLAT_CHARGE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RATE_STEP_FLAT_CHARGE, __dataOut);
    }
    if (null == this.UNIT_COST_USED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.UNIT_COST_USED, __dataOut);
    }
    if (null == this.CHARGE_BUNDLE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CHARGE_BUNDLE, __dataOut);
    }
    if (null == this.BASE_AMOUNT_FACTOR_USED) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.BASE_AMOUNT_FACTOR_USED, __dataOut);
    }
    if (null == this.REFUND_FACTOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REFUND_FACTOR, __dataOut);
    }
    if (null == this.CURRENCY) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CURRENCY);
    }
    if (null == this.CURRENCY_CONVERSION_RATE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.CURRENCY_CONVERSION_RATE, __dataOut);
    }
    if (null == this.BASE_UNIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, BASE_UNIT);
    }
    if (null == this.RATE_UNIT) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RATE_UNIT);
    }
    if (null == this.ROUNDED_UNIT_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.ROUNDED_UNIT_ID, __dataOut);
    }
    if (null == this.RECORD_TYPE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RECORD_TYPE, __dataOut);
    }
    if (null == this.LINK_FIELD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.LINK_FIELD, __dataOut);
    }
    if (null == this.REASON_FOR_CLEARDOWN) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REASON_FOR_CLEARDOWN, __dataOut);
    }
    if (null == this.REPAIR_INDICATOR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.REPAIR_INDICATOR, __dataOut);
    }
    if (null == this.RATED_BILLING_PERIOD) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.RATED_BILLING_PERIOD, __dataOut);
    }
    if (null == this.TRAFFIC_MOVEMENT_CTR) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.TRAFFIC_MOVEMENT_CTR, __dataOut);
    }
    if (null == this.RTE_LOOKUP_STYLE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, RTE_LOOKUP_STYLE);
    }
    if (null == this.CDR_SOURCE) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CDR_SOURCE);
    }
    if (null == this.CARRIER_DESTINATION_NAME) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CARRIER_DESTINATION_NAME);
    }
    if (null == this.CARRIER_DESTINATION_ALIAS) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CARRIER_DESTINATION_ALIAS);
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
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_FILE_NO==null?"null":CDR_FILE_NO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_UCI_NO==null?"null":CDR_UCI_NO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_UCI_ELEMENT==null?"null":CDR_UCI_ELEMENT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_KEY_ID==null?"null":CDR_KEY_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_SCENARIO==null?"null":CALL_SCENARIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REMUNERATION_INDICATOR==null?"null":REMUNERATION_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_BASE_DIRECTION==null?"null":BILLING_BASE_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FRANCHISE==null?"null":FRANCHISE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PRODUCT_GROUP==null?"null":PRODUCT_GROUP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVERSE_INDICATOR==null?"null":REVERSE_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_DIRECTION==null?"null":EVENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DERIVED_EVENT_DIRECTION==null?"null":DERIVED_EVENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_RATING_SCENARIO_TYPE==null?"null":BILLING_RATING_SCENARIO_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLED_PRODUCT==null?"null":BILLED_PRODUCT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATING_SCENARIO==null?"null":RATING_SCENARIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATING_RULE_DEPENDENCY_IND==null?"null":RATING_RULE_DEPENDENCY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ESTIMATE_INDICATOR==null?"null":ESTIMATE_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACCOUNTING_METHOD==null?"null":ACCOUNTING_METHOD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CASH_FLOW==null?"null":CASH_FLOW, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(COMPONENT_DIRECTION==null?"null":COMPONENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(STATEMENT_DIRECTION==null?"null":STATEMENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_METHOD==null?"null":BILLING_METHOD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATING_COMPONENT==null?"null":RATING_COMPONENT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_ATTEMPT_INDICATOR==null?"null":CALL_ATTEMPT_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PARAMETER_MATRIX==null?"null":PARAMETER_MATRIX, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PARAMETER_MATRIX_LINE==null?"null":PARAMETER_MATRIX_LINE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(STEP_NUMBER==null?"null":STEP_NUMBER.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_TYPE==null?"null":RATE_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIME_PREMIUM==null?"null":TIME_PREMIUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_OPERATOR==null?"null":BILLING_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_OWNER_OPERATOR==null?"null":RATE_OWNER_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SETTLEMENT_OPERATOR==null?"null":SETTLEMENT_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_NAME==null?"null":RATE_NAME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIER==null?"null":TIER, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIER_GROUP==null?"null":TIER_GROUP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIER_TYPE==null?"null":TIER_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OPTIMUM_POI==null?"null":OPTIMUM_POI, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADHOC_SUMMARY_IND==null?"null":ADHOC_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_SUMMARY_IND==null?"null":BILLING_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(COMPLEMENTARY_SUMMARY_IND==null?"null":COMPLEMENTARY_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_SUMMARY_IND==null?"null":TRAFFIC_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MINIMUM_CHARGE_IND==null?"null":MINIMUM_CHARGE_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_DURATION_BUCKET==null?"null":CALL_DURATION_BUCKET.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIME_OF_DAY_BUCKET==null?"null":TIME_OF_DAY_BUCKET.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_SUMMARISATION==null?"null":USER_SUMMARISATION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_NODE==null?"null":INCOMING_NODE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_PRODUCT==null?"null":INCOMING_PRODUCT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_OPERATOR==null?"null":INCOMING_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_PATH==null?"null":INCOMING_PATH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_POI==null?"null":INCOMING_POI, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_NODE==null?"null":OUTGOING_NODE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_PRODUCT==null?"null":OUTGOING_PRODUCT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_OPERATOR==null?"null":OUTGOING_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_PATH==null?"null":OUTGOING_PATH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_POI==null?"null":OUTGOING_POI, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_RATING_SCENARIO_TYPE==null?"null":TRAFFIC_RATING_SCENARIO_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_ROUTE_TYPE==null?"null":TRAFFIC_ROUTE_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_ROUTE==null?"null":TRAFFIC_ROUTE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_AGREEMENT_OPERATOR==null?"null":TRAFFIC_AGREEMENT_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_NEGOTIATION_DIR==null?"null":TRAFFIC_NEGOTIATION_DIR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_ROUTE_TYPE==null?"null":BILLING_ROUTE_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_ROUTE==null?"null":BILLING_ROUTE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_AGREEMENT_OPERATOR==null?"null":BILLING_AGREEMENT_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_NEGOTIATION_DIR==null?"null":BILLING_NEGOTIATION_DIR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ROUTE_IDENTIFIER==null?"null":ROUTE_IDENTIFIER, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REFILE_INDICATOR==null?"null":REFILE_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_ANUM_LEVEL==null?"null":AGREEMENT_NAAG_ANUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_ANUM==null?"null":AGREEMENT_NAAG_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_BNUM_LEVEL==null?"null":AGREEMENT_NAAG_BNUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_BNUM==null?"null":AGREEMENT_NAAG_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_NAAG_ANUM==null?"null":WORLD_VIEW_NAAG_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_ANUM==null?"null":WORLD_VIEW_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_NAAG_BNUM==null?"null":WORLD_VIEW_NAAG_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_BNUM==null?"null":WORLD_VIEW_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_BASE_DIRECTION==null?"null":TRAFFIC_BASE_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT==null?"null":DATA_UNIT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT_2==null?"null":DATA_UNIT_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT_3==null?"null":DATA_UNIT_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT_4==null?"null":DATA_UNIT_4, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DISCRETE_RATING_PARAMETER_1==null?"null":DISCRETE_RATING_PARAMETER_1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DISCRETE_RATING_PARAMETER_2==null?"null":DISCRETE_RATING_PARAMETER_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DISCRETE_RATING_PARAMETER_3==null?"null":DISCRETE_RATING_PARAMETER_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_CURRENCY_1==null?"null":REVENUE_SHARE_CURRENCY_1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_CURRENCY_2==null?"null":REVENUE_SHARE_CURRENCY_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_CURRENCY_3==null?"null":REVENUE_SHARE_CURRENCY_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ANUM_OPERATOR==null?"null":ANUM_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ANUM_CNP==null?"null":ANUM_CNP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_NAAG==null?"null":TRAFFIC_NAAG, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NAAG_ANUM_LEVEL==null?"null":NAAG_ANUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECON_NAAG_ANUM==null?"null":RECON_NAAG_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_ADDRESS_AGGR_ANUM==null?"null":NETWORK_ADDRESS_AGGR_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_TYPE_ANUM==null?"null":NETWORK_TYPE_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNUM_OPERATOR==null?"null":BNUM_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNUM_CNP==null?"null":BNUM_CNP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NAAG_BNUM_LEVEL==null?"null":NAAG_BNUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_TYPE_BNUM==null?"null":NETWORK_TYPE_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECON_NAAG_BNUM==null?"null":RECON_NAAG_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_ADDRESS_AGGR_BNUM==null?"null":NETWORK_ADDRESS_AGGR_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DERIVED_PRODUCT_INDICATOR==null?"null":DERIVED_PRODUCT_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ANUM==null?"null":ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNUM==null?"null":BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECORD_SEQUENCE_NUMBER==null?"null":RECORD_SEQUENCE_NUMBER, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_DATA==null?"null":USER_DATA, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_DATA_2==null?"null":USER_DATA_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_DATA_3==null?"null":USER_DATA_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_COUNT==null?"null":CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(START_CALL_COUNT==null?"null":START_CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_STEP_CALL_COUNT==null?"null":RATE_STEP_CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APPORTIONED_CALL_COUNT==null?"null":APPORTIONED_CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APPORTIONED_DURATION_SECONDS==null?"null":APPORTIONED_DURATION_SECONDS.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACTUAL_USAGE==null?"null":ACTUAL_USAGE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHARGED_USAGE==null?"null":CHARGED_USAGE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHARGED_UNITS==null?"null":CHARGED_UNITS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_DURATION==null?"null":EVENT_DURATION.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MINIMUM_CHARGE_ADJUSTMENT==null?"null":MINIMUM_CHARGE_ADJUSTMENT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MAXIMUM_CHARGE_ADJUSTMENT==null?"null":MAXIMUM_CHARGE_ADJUSTMENT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_DURATION==null?"null":NETWORK_DURATION.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME==null?"null":DATA_VOLUME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME_2==null?"null":DATA_VOLUME_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME_3==null?"null":DATA_VOLUME_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME_4==null?"null":DATA_VOLUME_4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_AMOUNT_1==null?"null":REVENUE_SHARE_AMOUNT_1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_AMOUNT_2==null?"null":REVENUE_SHARE_AMOUNT_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_AMOUNT_3==null?"null":REVENUE_SHARE_AMOUNT_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BASE_AMOUNT==null?"null":BASE_AMOUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AMOUNT==null?"null":AMOUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DLYS_DETAIL_ID==null?"null":DLYS_DETAIL_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_PERIOD==null?"null":TRAFFIC_PERIOD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MESSAGE_DATE==null?"null":"" + MESSAGE_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_DATE==null?"null":"" + BILLING_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADJUSTED_DATE==null?"null":"" + ADJUSTED_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROCESS_DATE==null?"null":"" + PROCESS_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_START_DATE==null?"null":"" + EVENT_START_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_START_TIME==null?"null":EVENT_START_TIME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_START_DATE==null?"null":NETWORK_START_DATE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_START_TIME==null?"null":NETWORK_START_TIME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_START_TIME==null?"null":BILLING_START_TIME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_END_TIME==null?"null":BILLING_END_TIME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FLAT_RATE_CHARGE==null?"null":FLAT_RATE_CHARGE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_STEP_FLAT_CHARGE==null?"null":RATE_STEP_FLAT_CHARGE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UNIT_COST_USED==null?"null":UNIT_COST_USED.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHARGE_BUNDLE==null?"null":CHARGE_BUNDLE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BASE_AMOUNT_FACTOR_USED==null?"null":BASE_AMOUNT_FACTOR_USED.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REFUND_FACTOR==null?"null":REFUND_FACTOR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CURRENCY==null?"null":CURRENCY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CURRENCY_CONVERSION_RATE==null?"null":CURRENCY_CONVERSION_RATE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BASE_UNIT==null?"null":BASE_UNIT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_UNIT==null?"null":RATE_UNIT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ROUNDED_UNIT_ID==null?"null":ROUNDED_UNIT_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECORD_TYPE==null?"null":RECORD_TYPE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LINK_FIELD==null?"null":LINK_FIELD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REASON_FOR_CLEARDOWN==null?"null":REASON_FOR_CLEARDOWN.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REPAIR_INDICATOR==null?"null":REPAIR_INDICATOR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATED_BILLING_PERIOD==null?"null":RATED_BILLING_PERIOD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_MOVEMENT_CTR==null?"null":TRAFFIC_MOVEMENT_CTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RTE_LOOKUP_STYLE==null?"null":RTE_LOOKUP_STYLE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_SOURCE==null?"null":CDR_SOURCE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CARRIER_DESTINATION_NAME==null?"null":CARRIER_DESTINATION_NAME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CARRIER_DESTINATION_ALIAS==null?"null":CARRIER_DESTINATION_ALIAS, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_FILE_NO==null?"null":CDR_FILE_NO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_UCI_NO==null?"null":CDR_UCI_NO.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_UCI_ELEMENT==null?"null":CDR_UCI_ELEMENT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_KEY_ID==null?"null":CDR_KEY_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_SCENARIO==null?"null":CALL_SCENARIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REMUNERATION_INDICATOR==null?"null":REMUNERATION_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_BASE_DIRECTION==null?"null":BILLING_BASE_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FRANCHISE==null?"null":FRANCHISE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PRODUCT_GROUP==null?"null":PRODUCT_GROUP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVERSE_INDICATOR==null?"null":REVERSE_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_DIRECTION==null?"null":EVENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DERIVED_EVENT_DIRECTION==null?"null":DERIVED_EVENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_RATING_SCENARIO_TYPE==null?"null":BILLING_RATING_SCENARIO_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLED_PRODUCT==null?"null":BILLED_PRODUCT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATING_SCENARIO==null?"null":RATING_SCENARIO, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATING_RULE_DEPENDENCY_IND==null?"null":RATING_RULE_DEPENDENCY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ESTIMATE_INDICATOR==null?"null":ESTIMATE_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACCOUNTING_METHOD==null?"null":ACCOUNTING_METHOD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CASH_FLOW==null?"null":CASH_FLOW, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(COMPONENT_DIRECTION==null?"null":COMPONENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(STATEMENT_DIRECTION==null?"null":STATEMENT_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_METHOD==null?"null":BILLING_METHOD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATING_COMPONENT==null?"null":RATING_COMPONENT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_ATTEMPT_INDICATOR==null?"null":CALL_ATTEMPT_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PARAMETER_MATRIX==null?"null":PARAMETER_MATRIX, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PARAMETER_MATRIX_LINE==null?"null":PARAMETER_MATRIX_LINE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(STEP_NUMBER==null?"null":STEP_NUMBER.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_TYPE==null?"null":RATE_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIME_PREMIUM==null?"null":TIME_PREMIUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_OPERATOR==null?"null":BILLING_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_OWNER_OPERATOR==null?"null":RATE_OWNER_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(SETTLEMENT_OPERATOR==null?"null":SETTLEMENT_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_NAME==null?"null":RATE_NAME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIER==null?"null":TIER, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIER_GROUP==null?"null":TIER_GROUP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIER_TYPE==null?"null":TIER_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OPTIMUM_POI==null?"null":OPTIMUM_POI, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADHOC_SUMMARY_IND==null?"null":ADHOC_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_SUMMARY_IND==null?"null":BILLING_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(COMPLEMENTARY_SUMMARY_IND==null?"null":COMPLEMENTARY_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_SUMMARY_IND==null?"null":TRAFFIC_SUMMARY_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MINIMUM_CHARGE_IND==null?"null":MINIMUM_CHARGE_IND, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_DURATION_BUCKET==null?"null":CALL_DURATION_BUCKET.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TIME_OF_DAY_BUCKET==null?"null":TIME_OF_DAY_BUCKET.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_SUMMARISATION==null?"null":USER_SUMMARISATION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_NODE==null?"null":INCOMING_NODE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_PRODUCT==null?"null":INCOMING_PRODUCT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_OPERATOR==null?"null":INCOMING_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_PATH==null?"null":INCOMING_PATH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(INCOMING_POI==null?"null":INCOMING_POI, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_NODE==null?"null":OUTGOING_NODE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_PRODUCT==null?"null":OUTGOING_PRODUCT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_OPERATOR==null?"null":OUTGOING_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_PATH==null?"null":OUTGOING_PATH, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(OUTGOING_POI==null?"null":OUTGOING_POI, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_RATING_SCENARIO_TYPE==null?"null":TRAFFIC_RATING_SCENARIO_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_ROUTE_TYPE==null?"null":TRAFFIC_ROUTE_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_ROUTE==null?"null":TRAFFIC_ROUTE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_AGREEMENT_OPERATOR==null?"null":TRAFFIC_AGREEMENT_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_NEGOTIATION_DIR==null?"null":TRAFFIC_NEGOTIATION_DIR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_ROUTE_TYPE==null?"null":BILLING_ROUTE_TYPE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_ROUTE==null?"null":BILLING_ROUTE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_AGREEMENT_OPERATOR==null?"null":BILLING_AGREEMENT_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_NEGOTIATION_DIR==null?"null":BILLING_NEGOTIATION_DIR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ROUTE_IDENTIFIER==null?"null":ROUTE_IDENTIFIER, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REFILE_INDICATOR==null?"null":REFILE_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_ANUM_LEVEL==null?"null":AGREEMENT_NAAG_ANUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_ANUM==null?"null":AGREEMENT_NAAG_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_BNUM_LEVEL==null?"null":AGREEMENT_NAAG_BNUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AGREEMENT_NAAG_BNUM==null?"null":AGREEMENT_NAAG_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_NAAG_ANUM==null?"null":WORLD_VIEW_NAAG_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_ANUM==null?"null":WORLD_VIEW_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_NAAG_BNUM==null?"null":WORLD_VIEW_NAAG_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(WORLD_VIEW_BNUM==null?"null":WORLD_VIEW_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_BASE_DIRECTION==null?"null":TRAFFIC_BASE_DIRECTION, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT==null?"null":DATA_UNIT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT_2==null?"null":DATA_UNIT_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT_3==null?"null":DATA_UNIT_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_UNIT_4==null?"null":DATA_UNIT_4, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DISCRETE_RATING_PARAMETER_1==null?"null":DISCRETE_RATING_PARAMETER_1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DISCRETE_RATING_PARAMETER_2==null?"null":DISCRETE_RATING_PARAMETER_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DISCRETE_RATING_PARAMETER_3==null?"null":DISCRETE_RATING_PARAMETER_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_CURRENCY_1==null?"null":REVENUE_SHARE_CURRENCY_1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_CURRENCY_2==null?"null":REVENUE_SHARE_CURRENCY_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_CURRENCY_3==null?"null":REVENUE_SHARE_CURRENCY_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ANUM_OPERATOR==null?"null":ANUM_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ANUM_CNP==null?"null":ANUM_CNP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_NAAG==null?"null":TRAFFIC_NAAG, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NAAG_ANUM_LEVEL==null?"null":NAAG_ANUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECON_NAAG_ANUM==null?"null":RECON_NAAG_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_ADDRESS_AGGR_ANUM==null?"null":NETWORK_ADDRESS_AGGR_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_TYPE_ANUM==null?"null":NETWORK_TYPE_ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNUM_OPERATOR==null?"null":BNUM_OPERATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNUM_CNP==null?"null":BNUM_CNP, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NAAG_BNUM_LEVEL==null?"null":NAAG_BNUM_LEVEL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_TYPE_BNUM==null?"null":NETWORK_TYPE_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECON_NAAG_BNUM==null?"null":RECON_NAAG_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_ADDRESS_AGGR_BNUM==null?"null":NETWORK_ADDRESS_AGGR_BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DERIVED_PRODUCT_INDICATOR==null?"null":DERIVED_PRODUCT_INDICATOR, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ANUM==null?"null":ANUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BNUM==null?"null":BNUM, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECORD_SEQUENCE_NUMBER==null?"null":RECORD_SEQUENCE_NUMBER, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_DATA==null?"null":USER_DATA, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_DATA_2==null?"null":USER_DATA_2, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(USER_DATA_3==null?"null":USER_DATA_3, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CALL_COUNT==null?"null":CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(START_CALL_COUNT==null?"null":START_CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_STEP_CALL_COUNT==null?"null":RATE_STEP_CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APPORTIONED_CALL_COUNT==null?"null":APPORTIONED_CALL_COUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(APPORTIONED_DURATION_SECONDS==null?"null":APPORTIONED_DURATION_SECONDS.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ACTUAL_USAGE==null?"null":ACTUAL_USAGE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHARGED_USAGE==null?"null":CHARGED_USAGE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHARGED_UNITS==null?"null":CHARGED_UNITS, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_DURATION==null?"null":EVENT_DURATION.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MINIMUM_CHARGE_ADJUSTMENT==null?"null":MINIMUM_CHARGE_ADJUSTMENT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MAXIMUM_CHARGE_ADJUSTMENT==null?"null":MAXIMUM_CHARGE_ADJUSTMENT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_DURATION==null?"null":NETWORK_DURATION.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME==null?"null":DATA_VOLUME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME_2==null?"null":DATA_VOLUME_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME_3==null?"null":DATA_VOLUME_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DATA_VOLUME_4==null?"null":DATA_VOLUME_4.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_AMOUNT_1==null?"null":REVENUE_SHARE_AMOUNT_1.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_AMOUNT_2==null?"null":REVENUE_SHARE_AMOUNT_2.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REVENUE_SHARE_AMOUNT_3==null?"null":REVENUE_SHARE_AMOUNT_3.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BASE_AMOUNT==null?"null":BASE_AMOUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(AMOUNT==null?"null":AMOUNT.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(DLYS_DETAIL_ID==null?"null":DLYS_DETAIL_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_PERIOD==null?"null":TRAFFIC_PERIOD, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MESSAGE_DATE==null?"null":"" + MESSAGE_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_DATE==null?"null":"" + BILLING_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ADJUSTED_DATE==null?"null":"" + ADJUSTED_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(PROCESS_DATE==null?"null":"" + PROCESS_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_START_DATE==null?"null":"" + EVENT_START_DATE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EVENT_START_TIME==null?"null":EVENT_START_TIME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_START_DATE==null?"null":NETWORK_START_DATE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(NETWORK_START_TIME==null?"null":NETWORK_START_TIME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_START_TIME==null?"null":BILLING_START_TIME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BILLING_END_TIME==null?"null":BILLING_END_TIME.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FLAT_RATE_CHARGE==null?"null":FLAT_RATE_CHARGE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_STEP_FLAT_CHARGE==null?"null":RATE_STEP_FLAT_CHARGE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(UNIT_COST_USED==null?"null":UNIT_COST_USED.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CHARGE_BUNDLE==null?"null":CHARGE_BUNDLE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BASE_AMOUNT_FACTOR_USED==null?"null":BASE_AMOUNT_FACTOR_USED.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REFUND_FACTOR==null?"null":REFUND_FACTOR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CURRENCY==null?"null":CURRENCY, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CURRENCY_CONVERSION_RATE==null?"null":CURRENCY_CONVERSION_RATE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(BASE_UNIT==null?"null":BASE_UNIT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATE_UNIT==null?"null":RATE_UNIT, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ROUNDED_UNIT_ID==null?"null":ROUNDED_UNIT_ID.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RECORD_TYPE==null?"null":RECORD_TYPE.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(LINK_FIELD==null?"null":LINK_FIELD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REASON_FOR_CLEARDOWN==null?"null":REASON_FOR_CLEARDOWN.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(REPAIR_INDICATOR==null?"null":REPAIR_INDICATOR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RATED_BILLING_PERIOD==null?"null":RATED_BILLING_PERIOD.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TRAFFIC_MOVEMENT_CTR==null?"null":TRAFFIC_MOVEMENT_CTR.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(RTE_LOOKUP_STYLE==null?"null":RTE_LOOKUP_STYLE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CDR_SOURCE==null?"null":CDR_SOURCE, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CARRIER_DESTINATION_NAME==null?"null":CARRIER_DESTINATION_NAME, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(CARRIER_DESTINATION_ALIAS==null?"null":CARRIER_DESTINATION_ALIAS, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CDR_FILE_NO = null; } else {
      this.CDR_FILE_NO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CDR_UCI_NO = null; } else {
      this.CDR_UCI_NO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CDR_UCI_ELEMENT = null; } else {
      this.CDR_UCI_ELEMENT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CDR_KEY_ID = null; } else {
      this.CDR_KEY_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CALL_SCENARIO = null; } else {
      this.CALL_SCENARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REMUNERATION_INDICATOR = null; } else {
      this.REMUNERATION_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_BASE_DIRECTION = null; } else {
      this.BILLING_BASE_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FRANCHISE = null; } else {
      this.FRANCHISE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRODUCT_GROUP = null; } else {
      this.PRODUCT_GROUP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVERSE_INDICATOR = null; } else {
      this.REVERSE_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EVENT_DIRECTION = null; } else {
      this.EVENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DERIVED_EVENT_DIRECTION = null; } else {
      this.DERIVED_EVENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_RATING_SCENARIO_TYPE = null; } else {
      this.BILLING_RATING_SCENARIO_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLED_PRODUCT = null; } else {
      this.BILLED_PRODUCT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATING_SCENARIO = null; } else {
      this.RATING_SCENARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATING_RULE_DEPENDENCY_IND = null; } else {
      this.RATING_RULE_DEPENDENCY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ESTIMATE_INDICATOR = null; } else {
      this.ESTIMATE_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ACCOUNTING_METHOD = null; } else {
      this.ACCOUNTING_METHOD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CASH_FLOW = null; } else {
      this.CASH_FLOW = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COMPONENT_DIRECTION = null; } else {
      this.COMPONENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.STATEMENT_DIRECTION = null; } else {
      this.STATEMENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_METHOD = null; } else {
      this.BILLING_METHOD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATING_COMPONENT = null; } else {
      this.RATING_COMPONENT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CALL_ATTEMPT_INDICATOR = null; } else {
      this.CALL_ATTEMPT_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PARAMETER_MATRIX = null; } else {
      this.PARAMETER_MATRIX = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PARAMETER_MATRIX_LINE = null; } else {
      this.PARAMETER_MATRIX_LINE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.STEP_NUMBER = null; } else {
      this.STEP_NUMBER = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_TYPE = null; } else {
      this.RATE_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIME_PREMIUM = null; } else {
      this.TIME_PREMIUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_OPERATOR = null; } else {
      this.BILLING_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_OWNER_OPERATOR = null; } else {
      this.RATE_OWNER_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SETTLEMENT_OPERATOR = null; } else {
      this.SETTLEMENT_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_NAME = null; } else {
      this.RATE_NAME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIER = null; } else {
      this.TIER = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIER_GROUP = null; } else {
      this.TIER_GROUP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIER_TYPE = null; } else {
      this.TIER_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OPTIMUM_POI = null; } else {
      this.OPTIMUM_POI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ADHOC_SUMMARY_IND = null; } else {
      this.ADHOC_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_SUMMARY_IND = null; } else {
      this.BILLING_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COMPLEMENTARY_SUMMARY_IND = null; } else {
      this.COMPLEMENTARY_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_SUMMARY_IND = null; } else {
      this.TRAFFIC_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MINIMUM_CHARGE_IND = null; } else {
      this.MINIMUM_CHARGE_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CALL_DURATION_BUCKET = null; } else {
      this.CALL_DURATION_BUCKET = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TIME_OF_DAY_BUCKET = null; } else {
      this.TIME_OF_DAY_BUCKET = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_SUMMARISATION = null; } else {
      this.USER_SUMMARISATION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_NODE = null; } else {
      this.INCOMING_NODE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_PRODUCT = null; } else {
      this.INCOMING_PRODUCT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_OPERATOR = null; } else {
      this.INCOMING_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_PATH = null; } else {
      this.INCOMING_PATH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_POI = null; } else {
      this.INCOMING_POI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_NODE = null; } else {
      this.OUTGOING_NODE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_PRODUCT = null; } else {
      this.OUTGOING_PRODUCT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_OPERATOR = null; } else {
      this.OUTGOING_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_PATH = null; } else {
      this.OUTGOING_PATH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_POI = null; } else {
      this.OUTGOING_POI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_RATING_SCENARIO_TYPE = null; } else {
      this.TRAFFIC_RATING_SCENARIO_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_ROUTE_TYPE = null; } else {
      this.TRAFFIC_ROUTE_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_ROUTE = null; } else {
      this.TRAFFIC_ROUTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_AGREEMENT_OPERATOR = null; } else {
      this.TRAFFIC_AGREEMENT_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_NEGOTIATION_DIR = null; } else {
      this.TRAFFIC_NEGOTIATION_DIR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_ROUTE_TYPE = null; } else {
      this.BILLING_ROUTE_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_ROUTE = null; } else {
      this.BILLING_ROUTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_AGREEMENT_OPERATOR = null; } else {
      this.BILLING_AGREEMENT_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_NEGOTIATION_DIR = null; } else {
      this.BILLING_NEGOTIATION_DIR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ROUTE_IDENTIFIER = null; } else {
      this.ROUTE_IDENTIFIER = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REFILE_INDICATOR = null; } else {
      this.REFILE_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_ANUM_LEVEL = null; } else {
      this.AGREEMENT_NAAG_ANUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_ANUM = null; } else {
      this.AGREEMENT_NAAG_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_BNUM_LEVEL = null; } else {
      this.AGREEMENT_NAAG_BNUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_BNUM = null; } else {
      this.AGREEMENT_NAAG_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_NAAG_ANUM = null; } else {
      this.WORLD_VIEW_NAAG_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_ANUM = null; } else {
      this.WORLD_VIEW_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_NAAG_BNUM = null; } else {
      this.WORLD_VIEW_NAAG_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_BNUM = null; } else {
      this.WORLD_VIEW_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_BASE_DIRECTION = null; } else {
      this.TRAFFIC_BASE_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT = null; } else {
      this.DATA_UNIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT_2 = null; } else {
      this.DATA_UNIT_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT_3 = null; } else {
      this.DATA_UNIT_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT_4 = null; } else {
      this.DATA_UNIT_4 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DISCRETE_RATING_PARAMETER_1 = null; } else {
      this.DISCRETE_RATING_PARAMETER_1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DISCRETE_RATING_PARAMETER_2 = null; } else {
      this.DISCRETE_RATING_PARAMETER_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DISCRETE_RATING_PARAMETER_3 = null; } else {
      this.DISCRETE_RATING_PARAMETER_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVENUE_SHARE_CURRENCY_1 = null; } else {
      this.REVENUE_SHARE_CURRENCY_1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVENUE_SHARE_CURRENCY_2 = null; } else {
      this.REVENUE_SHARE_CURRENCY_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVENUE_SHARE_CURRENCY_3 = null; } else {
      this.REVENUE_SHARE_CURRENCY_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANUM_OPERATOR = null; } else {
      this.ANUM_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANUM_CNP = null; } else {
      this.ANUM_CNP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_NAAG = null; } else {
      this.TRAFFIC_NAAG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NAAG_ANUM_LEVEL = null; } else {
      this.NAAG_ANUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECON_NAAG_ANUM = null; } else {
      this.RECON_NAAG_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_ADDRESS_AGGR_ANUM = null; } else {
      this.NETWORK_ADDRESS_AGGR_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_TYPE_ANUM = null; } else {
      this.NETWORK_TYPE_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNUM_OPERATOR = null; } else {
      this.BNUM_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNUM_CNP = null; } else {
      this.BNUM_CNP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NAAG_BNUM_LEVEL = null; } else {
      this.NAAG_BNUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_TYPE_BNUM = null; } else {
      this.NETWORK_TYPE_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECON_NAAG_BNUM = null; } else {
      this.RECON_NAAG_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_ADDRESS_AGGR_BNUM = null; } else {
      this.NETWORK_ADDRESS_AGGR_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DERIVED_PRODUCT_INDICATOR = null; } else {
      this.DERIVED_PRODUCT_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANUM = null; } else {
      this.ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNUM = null; } else {
      this.BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECORD_SEQUENCE_NUMBER = null; } else {
      this.RECORD_SEQUENCE_NUMBER = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_DATA = null; } else {
      this.USER_DATA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_DATA_2 = null; } else {
      this.USER_DATA_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_DATA_3 = null; } else {
      this.USER_DATA_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CALL_COUNT = null; } else {
      this.CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.START_CALL_COUNT = null; } else {
      this.START_CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RATE_STEP_CALL_COUNT = null; } else {
      this.RATE_STEP_CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.APPORTIONED_CALL_COUNT = null; } else {
      this.APPORTIONED_CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.APPORTIONED_DURATION_SECONDS = null; } else {
      this.APPORTIONED_DURATION_SECONDS = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ACTUAL_USAGE = null; } else {
      this.ACTUAL_USAGE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CHARGED_USAGE = null; } else {
      this.CHARGED_USAGE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHARGED_UNITS = null; } else {
      this.CHARGED_UNITS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EVENT_DURATION = null; } else {
      this.EVENT_DURATION = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.MINIMUM_CHARGE_ADJUSTMENT = null; } else {
      this.MINIMUM_CHARGE_ADJUSTMENT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.MAXIMUM_CHARGE_ADJUSTMENT = null; } else {
      this.MAXIMUM_CHARGE_ADJUSTMENT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NETWORK_DURATION = null; } else {
      this.NETWORK_DURATION = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME = null; } else {
      this.DATA_VOLUME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME_2 = null; } else {
      this.DATA_VOLUME_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME_3 = null; } else {
      this.DATA_VOLUME_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME_4 = null; } else {
      this.DATA_VOLUME_4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REVENUE_SHARE_AMOUNT_1 = null; } else {
      this.REVENUE_SHARE_AMOUNT_1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REVENUE_SHARE_AMOUNT_2 = null; } else {
      this.REVENUE_SHARE_AMOUNT_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REVENUE_SHARE_AMOUNT_3 = null; } else {
      this.REVENUE_SHARE_AMOUNT_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BASE_AMOUNT = null; } else {
      this.BASE_AMOUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AMOUNT = null; } else {
      this.AMOUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DLYS_DETAIL_ID = null; } else {
      this.DLYS_DETAIL_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_PERIOD = null; } else {
      this.TRAFFIC_PERIOD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.MESSAGE_DATE = null; } else {
      this.MESSAGE_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLING_DATE = null; } else {
      this.BILLING_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ADJUSTED_DATE = null; } else {
      this.ADJUSTED_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PROCESS_DATE = null; } else {
      this.PROCESS_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EVENT_START_DATE = null; } else {
      this.EVENT_START_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EVENT_START_TIME = null; } else {
      this.EVENT_START_TIME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NETWORK_START_DATE = null; } else {
      this.NETWORK_START_DATE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NETWORK_START_TIME = null; } else {
      this.NETWORK_START_TIME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLING_START_TIME = null; } else {
      this.BILLING_START_TIME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLING_END_TIME = null; } else {
      this.BILLING_END_TIME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FLAT_RATE_CHARGE = null; } else {
      this.FLAT_RATE_CHARGE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RATE_STEP_FLAT_CHARGE = null; } else {
      this.RATE_STEP_FLAT_CHARGE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.UNIT_COST_USED = null; } else {
      this.UNIT_COST_USED = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CHARGE_BUNDLE = null; } else {
      this.CHARGE_BUNDLE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BASE_AMOUNT_FACTOR_USED = null; } else {
      this.BASE_AMOUNT_FACTOR_USED = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REFUND_FACTOR = null; } else {
      this.REFUND_FACTOR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CURRENCY = null; } else {
      this.CURRENCY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CURRENCY_CONVERSION_RATE = null; } else {
      this.CURRENCY_CONVERSION_RATE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BASE_UNIT = null; } else {
      this.BASE_UNIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_UNIT = null; } else {
      this.RATE_UNIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ROUNDED_UNIT_ID = null; } else {
      this.ROUNDED_UNIT_ID = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RECORD_TYPE = null; } else {
      this.RECORD_TYPE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.LINK_FIELD = null; } else {
      this.LINK_FIELD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REASON_FOR_CLEARDOWN = null; } else {
      this.REASON_FOR_CLEARDOWN = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REPAIR_INDICATOR = null; } else {
      this.REPAIR_INDICATOR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RATED_BILLING_PERIOD = null; } else {
      this.RATED_BILLING_PERIOD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TRAFFIC_MOVEMENT_CTR = null; } else {
      this.TRAFFIC_MOVEMENT_CTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RTE_LOOKUP_STYLE = null; } else {
      this.RTE_LOOKUP_STYLE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CDR_SOURCE = null; } else {
      this.CDR_SOURCE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CARRIER_DESTINATION_NAME = null; } else {
      this.CARRIER_DESTINATION_NAME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CARRIER_DESTINATION_ALIAS = null; } else {
      this.CARRIER_DESTINATION_ALIAS = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CDR_FILE_NO = null; } else {
      this.CDR_FILE_NO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CDR_UCI_NO = null; } else {
      this.CDR_UCI_NO = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CDR_UCI_ELEMENT = null; } else {
      this.CDR_UCI_ELEMENT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CDR_KEY_ID = null; } else {
      this.CDR_KEY_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CALL_SCENARIO = null; } else {
      this.CALL_SCENARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REMUNERATION_INDICATOR = null; } else {
      this.REMUNERATION_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_BASE_DIRECTION = null; } else {
      this.BILLING_BASE_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FRANCHISE = null; } else {
      this.FRANCHISE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PRODUCT_GROUP = null; } else {
      this.PRODUCT_GROUP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVERSE_INDICATOR = null; } else {
      this.REVERSE_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EVENT_DIRECTION = null; } else {
      this.EVENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DERIVED_EVENT_DIRECTION = null; } else {
      this.DERIVED_EVENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_RATING_SCENARIO_TYPE = null; } else {
      this.BILLING_RATING_SCENARIO_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLED_PRODUCT = null; } else {
      this.BILLED_PRODUCT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATING_SCENARIO = null; } else {
      this.RATING_SCENARIO = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATING_RULE_DEPENDENCY_IND = null; } else {
      this.RATING_RULE_DEPENDENCY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ESTIMATE_INDICATOR = null; } else {
      this.ESTIMATE_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ACCOUNTING_METHOD = null; } else {
      this.ACCOUNTING_METHOD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CASH_FLOW = null; } else {
      this.CASH_FLOW = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COMPONENT_DIRECTION = null; } else {
      this.COMPONENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.STATEMENT_DIRECTION = null; } else {
      this.STATEMENT_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_METHOD = null; } else {
      this.BILLING_METHOD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATING_COMPONENT = null; } else {
      this.RATING_COMPONENT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CALL_ATTEMPT_INDICATOR = null; } else {
      this.CALL_ATTEMPT_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.PARAMETER_MATRIX = null; } else {
      this.PARAMETER_MATRIX = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PARAMETER_MATRIX_LINE = null; } else {
      this.PARAMETER_MATRIX_LINE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.STEP_NUMBER = null; } else {
      this.STEP_NUMBER = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_TYPE = null; } else {
      this.RATE_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIME_PREMIUM = null; } else {
      this.TIME_PREMIUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_OPERATOR = null; } else {
      this.BILLING_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_OWNER_OPERATOR = null; } else {
      this.RATE_OWNER_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.SETTLEMENT_OPERATOR = null; } else {
      this.SETTLEMENT_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_NAME = null; } else {
      this.RATE_NAME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIER = null; } else {
      this.TIER = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIER_GROUP = null; } else {
      this.TIER_GROUP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TIER_TYPE = null; } else {
      this.TIER_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OPTIMUM_POI = null; } else {
      this.OPTIMUM_POI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ADHOC_SUMMARY_IND = null; } else {
      this.ADHOC_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_SUMMARY_IND = null; } else {
      this.BILLING_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.COMPLEMENTARY_SUMMARY_IND = null; } else {
      this.COMPLEMENTARY_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_SUMMARY_IND = null; } else {
      this.TRAFFIC_SUMMARY_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.MINIMUM_CHARGE_IND = null; } else {
      this.MINIMUM_CHARGE_IND = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CALL_DURATION_BUCKET = null; } else {
      this.CALL_DURATION_BUCKET = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TIME_OF_DAY_BUCKET = null; } else {
      this.TIME_OF_DAY_BUCKET = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_SUMMARISATION = null; } else {
      this.USER_SUMMARISATION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_NODE = null; } else {
      this.INCOMING_NODE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_PRODUCT = null; } else {
      this.INCOMING_PRODUCT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_OPERATOR = null; } else {
      this.INCOMING_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_PATH = null; } else {
      this.INCOMING_PATH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.INCOMING_POI = null; } else {
      this.INCOMING_POI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_NODE = null; } else {
      this.OUTGOING_NODE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_PRODUCT = null; } else {
      this.OUTGOING_PRODUCT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_OPERATOR = null; } else {
      this.OUTGOING_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_PATH = null; } else {
      this.OUTGOING_PATH = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.OUTGOING_POI = null; } else {
      this.OUTGOING_POI = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_RATING_SCENARIO_TYPE = null; } else {
      this.TRAFFIC_RATING_SCENARIO_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_ROUTE_TYPE = null; } else {
      this.TRAFFIC_ROUTE_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_ROUTE = null; } else {
      this.TRAFFIC_ROUTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_AGREEMENT_OPERATOR = null; } else {
      this.TRAFFIC_AGREEMENT_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_NEGOTIATION_DIR = null; } else {
      this.TRAFFIC_NEGOTIATION_DIR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_ROUTE_TYPE = null; } else {
      this.BILLING_ROUTE_TYPE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_ROUTE = null; } else {
      this.BILLING_ROUTE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_AGREEMENT_OPERATOR = null; } else {
      this.BILLING_AGREEMENT_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BILLING_NEGOTIATION_DIR = null; } else {
      this.BILLING_NEGOTIATION_DIR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ROUTE_IDENTIFIER = null; } else {
      this.ROUTE_IDENTIFIER = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REFILE_INDICATOR = null; } else {
      this.REFILE_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_ANUM_LEVEL = null; } else {
      this.AGREEMENT_NAAG_ANUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_ANUM = null; } else {
      this.AGREEMENT_NAAG_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_BNUM_LEVEL = null; } else {
      this.AGREEMENT_NAAG_BNUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.AGREEMENT_NAAG_BNUM = null; } else {
      this.AGREEMENT_NAAG_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_NAAG_ANUM = null; } else {
      this.WORLD_VIEW_NAAG_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_ANUM = null; } else {
      this.WORLD_VIEW_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_NAAG_BNUM = null; } else {
      this.WORLD_VIEW_NAAG_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.WORLD_VIEW_BNUM = null; } else {
      this.WORLD_VIEW_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_BASE_DIRECTION = null; } else {
      this.TRAFFIC_BASE_DIRECTION = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT = null; } else {
      this.DATA_UNIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT_2 = null; } else {
      this.DATA_UNIT_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT_3 = null; } else {
      this.DATA_UNIT_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DATA_UNIT_4 = null; } else {
      this.DATA_UNIT_4 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DISCRETE_RATING_PARAMETER_1 = null; } else {
      this.DISCRETE_RATING_PARAMETER_1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DISCRETE_RATING_PARAMETER_2 = null; } else {
      this.DISCRETE_RATING_PARAMETER_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DISCRETE_RATING_PARAMETER_3 = null; } else {
      this.DISCRETE_RATING_PARAMETER_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVENUE_SHARE_CURRENCY_1 = null; } else {
      this.REVENUE_SHARE_CURRENCY_1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVENUE_SHARE_CURRENCY_2 = null; } else {
      this.REVENUE_SHARE_CURRENCY_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.REVENUE_SHARE_CURRENCY_3 = null; } else {
      this.REVENUE_SHARE_CURRENCY_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANUM_OPERATOR = null; } else {
      this.ANUM_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANUM_CNP = null; } else {
      this.ANUM_CNP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_NAAG = null; } else {
      this.TRAFFIC_NAAG = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NAAG_ANUM_LEVEL = null; } else {
      this.NAAG_ANUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECON_NAAG_ANUM = null; } else {
      this.RECON_NAAG_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_ADDRESS_AGGR_ANUM = null; } else {
      this.NETWORK_ADDRESS_AGGR_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_TYPE_ANUM = null; } else {
      this.NETWORK_TYPE_ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNUM_OPERATOR = null; } else {
      this.BNUM_OPERATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNUM_CNP = null; } else {
      this.BNUM_CNP = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NAAG_BNUM_LEVEL = null; } else {
      this.NAAG_BNUM_LEVEL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_TYPE_BNUM = null; } else {
      this.NETWORK_TYPE_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECON_NAAG_BNUM = null; } else {
      this.RECON_NAAG_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.NETWORK_ADDRESS_AGGR_BNUM = null; } else {
      this.NETWORK_ADDRESS_AGGR_BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DERIVED_PRODUCT_INDICATOR = null; } else {
      this.DERIVED_PRODUCT_INDICATOR = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.ANUM = null; } else {
      this.ANUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BNUM = null; } else {
      this.BNUM = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RECORD_SEQUENCE_NUMBER = null; } else {
      this.RECORD_SEQUENCE_NUMBER = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_DATA = null; } else {
      this.USER_DATA = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_DATA_2 = null; } else {
      this.USER_DATA_2 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.USER_DATA_3 = null; } else {
      this.USER_DATA_3 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CALL_COUNT = null; } else {
      this.CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.START_CALL_COUNT = null; } else {
      this.START_CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RATE_STEP_CALL_COUNT = null; } else {
      this.RATE_STEP_CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.APPORTIONED_CALL_COUNT = null; } else {
      this.APPORTIONED_CALL_COUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.APPORTIONED_DURATION_SECONDS = null; } else {
      this.APPORTIONED_DURATION_SECONDS = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ACTUAL_USAGE = null; } else {
      this.ACTUAL_USAGE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CHARGED_USAGE = null; } else {
      this.CHARGED_USAGE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CHARGED_UNITS = null; } else {
      this.CHARGED_UNITS = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EVENT_DURATION = null; } else {
      this.EVENT_DURATION = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.MINIMUM_CHARGE_ADJUSTMENT = null; } else {
      this.MINIMUM_CHARGE_ADJUSTMENT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.MAXIMUM_CHARGE_ADJUSTMENT = null; } else {
      this.MAXIMUM_CHARGE_ADJUSTMENT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NETWORK_DURATION = null; } else {
      this.NETWORK_DURATION = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME = null; } else {
      this.DATA_VOLUME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME_2 = null; } else {
      this.DATA_VOLUME_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME_3 = null; } else {
      this.DATA_VOLUME_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.DATA_VOLUME_4 = null; } else {
      this.DATA_VOLUME_4 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REVENUE_SHARE_AMOUNT_1 = null; } else {
      this.REVENUE_SHARE_AMOUNT_1 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REVENUE_SHARE_AMOUNT_2 = null; } else {
      this.REVENUE_SHARE_AMOUNT_2 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REVENUE_SHARE_AMOUNT_3 = null; } else {
      this.REVENUE_SHARE_AMOUNT_3 = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BASE_AMOUNT = null; } else {
      this.BASE_AMOUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.AMOUNT = null; } else {
      this.AMOUNT = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.DLYS_DETAIL_ID = null; } else {
      this.DLYS_DETAIL_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.TRAFFIC_PERIOD = null; } else {
      this.TRAFFIC_PERIOD = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.MESSAGE_DATE = null; } else {
      this.MESSAGE_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLING_DATE = null; } else {
      this.BILLING_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ADJUSTED_DATE = null; } else {
      this.ADJUSTED_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.PROCESS_DATE = null; } else {
      this.PROCESS_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.EVENT_START_DATE = null; } else {
      this.EVENT_START_DATE = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.EVENT_START_TIME = null; } else {
      this.EVENT_START_TIME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NETWORK_START_DATE = null; } else {
      this.NETWORK_START_DATE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.NETWORK_START_TIME = null; } else {
      this.NETWORK_START_TIME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLING_START_TIME = null; } else {
      this.BILLING_START_TIME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BILLING_END_TIME = null; } else {
      this.BILLING_END_TIME = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.FLAT_RATE_CHARGE = null; } else {
      this.FLAT_RATE_CHARGE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RATE_STEP_FLAT_CHARGE = null; } else {
      this.RATE_STEP_FLAT_CHARGE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.UNIT_COST_USED = null; } else {
      this.UNIT_COST_USED = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CHARGE_BUNDLE = null; } else {
      this.CHARGE_BUNDLE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.BASE_AMOUNT_FACTOR_USED = null; } else {
      this.BASE_AMOUNT_FACTOR_USED = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REFUND_FACTOR = null; } else {
      this.REFUND_FACTOR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CURRENCY = null; } else {
      this.CURRENCY = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.CURRENCY_CONVERSION_RATE = null; } else {
      this.CURRENCY_CONVERSION_RATE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.BASE_UNIT = null; } else {
      this.BASE_UNIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RATE_UNIT = null; } else {
      this.RATE_UNIT = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ROUNDED_UNIT_ID = null; } else {
      this.ROUNDED_UNIT_ID = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RECORD_TYPE = null; } else {
      this.RECORD_TYPE = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.LINK_FIELD = null; } else {
      this.LINK_FIELD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REASON_FOR_CLEARDOWN = null; } else {
      this.REASON_FOR_CLEARDOWN = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.REPAIR_INDICATOR = null; } else {
      this.REPAIR_INDICATOR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.RATED_BILLING_PERIOD = null; } else {
      this.RATED_BILLING_PERIOD = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.TRAFFIC_MOVEMENT_CTR = null; } else {
      this.TRAFFIC_MOVEMENT_CTR = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.RTE_LOOKUP_STYLE = null; } else {
      this.RTE_LOOKUP_STYLE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CDR_SOURCE = null; } else {
      this.CDR_SOURCE = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CARRIER_DESTINATION_NAME = null; } else {
      this.CARRIER_DESTINATION_NAME = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.CARRIER_DESTINATION_ALIAS = null; } else {
      this.CARRIER_DESTINATION_ALIAS = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.MESSAGE_DATE = (o.MESSAGE_DATE != null) ? (java.sql.Timestamp) o.MESSAGE_DATE.clone() : null;
    o.BILLING_DATE = (o.BILLING_DATE != null) ? (java.sql.Timestamp) o.BILLING_DATE.clone() : null;
    o.ADJUSTED_DATE = (o.ADJUSTED_DATE != null) ? (java.sql.Timestamp) o.ADJUSTED_DATE.clone() : null;
    o.PROCESS_DATE = (o.PROCESS_DATE != null) ? (java.sql.Timestamp) o.PROCESS_DATE.clone() : null;
    o.EVENT_START_DATE = (o.EVENT_START_DATE != null) ? (java.sql.Timestamp) o.EVENT_START_DATE.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.MESSAGE_DATE = (o.MESSAGE_DATE != null) ? (java.sql.Timestamp) o.MESSAGE_DATE.clone() : null;
    o.BILLING_DATE = (o.BILLING_DATE != null) ? (java.sql.Timestamp) o.BILLING_DATE.clone() : null;
    o.ADJUSTED_DATE = (o.ADJUSTED_DATE != null) ? (java.sql.Timestamp) o.ADJUSTED_DATE.clone() : null;
    o.PROCESS_DATE = (o.PROCESS_DATE != null) ? (java.sql.Timestamp) o.PROCESS_DATE.clone() : null;
    o.EVENT_START_DATE = (o.EVENT_START_DATE != null) ? (java.sql.Timestamp) o.EVENT_START_DATE.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("CDR_FILE_NO", this.CDR_FILE_NO);
    __sqoop$field_map.put("CDR_UCI_NO", this.CDR_UCI_NO);
    __sqoop$field_map.put("CDR_UCI_ELEMENT", this.CDR_UCI_ELEMENT);
    __sqoop$field_map.put("CDR_KEY_ID", this.CDR_KEY_ID);
    __sqoop$field_map.put("CALL_SCENARIO", this.CALL_SCENARIO);
    __sqoop$field_map.put("REMUNERATION_INDICATOR", this.REMUNERATION_INDICATOR);
    __sqoop$field_map.put("BILLING_BASE_DIRECTION", this.BILLING_BASE_DIRECTION);
    __sqoop$field_map.put("FRANCHISE", this.FRANCHISE);
    __sqoop$field_map.put("PRODUCT_GROUP", this.PRODUCT_GROUP);
    __sqoop$field_map.put("REVERSE_INDICATOR", this.REVERSE_INDICATOR);
    __sqoop$field_map.put("EVENT_DIRECTION", this.EVENT_DIRECTION);
    __sqoop$field_map.put("DERIVED_EVENT_DIRECTION", this.DERIVED_EVENT_DIRECTION);
    __sqoop$field_map.put("BILLING_RATING_SCENARIO_TYPE", this.BILLING_RATING_SCENARIO_TYPE);
    __sqoop$field_map.put("BILLED_PRODUCT", this.BILLED_PRODUCT);
    __sqoop$field_map.put("RATING_SCENARIO", this.RATING_SCENARIO);
    __sqoop$field_map.put("RATING_RULE_DEPENDENCY_IND", this.RATING_RULE_DEPENDENCY_IND);
    __sqoop$field_map.put("ESTIMATE_INDICATOR", this.ESTIMATE_INDICATOR);
    __sqoop$field_map.put("ACCOUNTING_METHOD", this.ACCOUNTING_METHOD);
    __sqoop$field_map.put("CASH_FLOW", this.CASH_FLOW);
    __sqoop$field_map.put("COMPONENT_DIRECTION", this.COMPONENT_DIRECTION);
    __sqoop$field_map.put("STATEMENT_DIRECTION", this.STATEMENT_DIRECTION);
    __sqoop$field_map.put("BILLING_METHOD", this.BILLING_METHOD);
    __sqoop$field_map.put("RATING_COMPONENT", this.RATING_COMPONENT);
    __sqoop$field_map.put("CALL_ATTEMPT_INDICATOR", this.CALL_ATTEMPT_INDICATOR);
    __sqoop$field_map.put("PARAMETER_MATRIX", this.PARAMETER_MATRIX);
    __sqoop$field_map.put("PARAMETER_MATRIX_LINE", this.PARAMETER_MATRIX_LINE);
    __sqoop$field_map.put("STEP_NUMBER", this.STEP_NUMBER);
    __sqoop$field_map.put("RATE_TYPE", this.RATE_TYPE);
    __sqoop$field_map.put("TIME_PREMIUM", this.TIME_PREMIUM);
    __sqoop$field_map.put("BILLING_OPERATOR", this.BILLING_OPERATOR);
    __sqoop$field_map.put("RATE_OWNER_OPERATOR", this.RATE_OWNER_OPERATOR);
    __sqoop$field_map.put("SETTLEMENT_OPERATOR", this.SETTLEMENT_OPERATOR);
    __sqoop$field_map.put("RATE_NAME", this.RATE_NAME);
    __sqoop$field_map.put("TIER", this.TIER);
    __sqoop$field_map.put("TIER_GROUP", this.TIER_GROUP);
    __sqoop$field_map.put("TIER_TYPE", this.TIER_TYPE);
    __sqoop$field_map.put("OPTIMUM_POI", this.OPTIMUM_POI);
    __sqoop$field_map.put("ADHOC_SUMMARY_IND", this.ADHOC_SUMMARY_IND);
    __sqoop$field_map.put("BILLING_SUMMARY_IND", this.BILLING_SUMMARY_IND);
    __sqoop$field_map.put("COMPLEMENTARY_SUMMARY_IND", this.COMPLEMENTARY_SUMMARY_IND);
    __sqoop$field_map.put("TRAFFIC_SUMMARY_IND", this.TRAFFIC_SUMMARY_IND);
    __sqoop$field_map.put("MINIMUM_CHARGE_IND", this.MINIMUM_CHARGE_IND);
    __sqoop$field_map.put("CALL_DURATION_BUCKET", this.CALL_DURATION_BUCKET);
    __sqoop$field_map.put("TIME_OF_DAY_BUCKET", this.TIME_OF_DAY_BUCKET);
    __sqoop$field_map.put("USER_SUMMARISATION", this.USER_SUMMARISATION);
    __sqoop$field_map.put("INCOMING_NODE", this.INCOMING_NODE);
    __sqoop$field_map.put("INCOMING_PRODUCT", this.INCOMING_PRODUCT);
    __sqoop$field_map.put("INCOMING_OPERATOR", this.INCOMING_OPERATOR);
    __sqoop$field_map.put("INCOMING_PATH", this.INCOMING_PATH);
    __sqoop$field_map.put("INCOMING_POI", this.INCOMING_POI);
    __sqoop$field_map.put("OUTGOING_NODE", this.OUTGOING_NODE);
    __sqoop$field_map.put("OUTGOING_PRODUCT", this.OUTGOING_PRODUCT);
    __sqoop$field_map.put("OUTGOING_OPERATOR", this.OUTGOING_OPERATOR);
    __sqoop$field_map.put("OUTGOING_PATH", this.OUTGOING_PATH);
    __sqoop$field_map.put("OUTGOING_POI", this.OUTGOING_POI);
    __sqoop$field_map.put("TRAFFIC_RATING_SCENARIO_TYPE", this.TRAFFIC_RATING_SCENARIO_TYPE);
    __sqoop$field_map.put("TRAFFIC_ROUTE_TYPE", this.TRAFFIC_ROUTE_TYPE);
    __sqoop$field_map.put("TRAFFIC_ROUTE", this.TRAFFIC_ROUTE);
    __sqoop$field_map.put("TRAFFIC_AGREEMENT_OPERATOR", this.TRAFFIC_AGREEMENT_OPERATOR);
    __sqoop$field_map.put("TRAFFIC_NEGOTIATION_DIR", this.TRAFFIC_NEGOTIATION_DIR);
    __sqoop$field_map.put("BILLING_ROUTE_TYPE", this.BILLING_ROUTE_TYPE);
    __sqoop$field_map.put("BILLING_ROUTE", this.BILLING_ROUTE);
    __sqoop$field_map.put("BILLING_AGREEMENT_OPERATOR", this.BILLING_AGREEMENT_OPERATOR);
    __sqoop$field_map.put("BILLING_NEGOTIATION_DIR", this.BILLING_NEGOTIATION_DIR);
    __sqoop$field_map.put("ROUTE_IDENTIFIER", this.ROUTE_IDENTIFIER);
    __sqoop$field_map.put("REFILE_INDICATOR", this.REFILE_INDICATOR);
    __sqoop$field_map.put("AGREEMENT_NAAG_ANUM_LEVEL", this.AGREEMENT_NAAG_ANUM_LEVEL);
    __sqoop$field_map.put("AGREEMENT_NAAG_ANUM", this.AGREEMENT_NAAG_ANUM);
    __sqoop$field_map.put("AGREEMENT_NAAG_BNUM_LEVEL", this.AGREEMENT_NAAG_BNUM_LEVEL);
    __sqoop$field_map.put("AGREEMENT_NAAG_BNUM", this.AGREEMENT_NAAG_BNUM);
    __sqoop$field_map.put("WORLD_VIEW_NAAG_ANUM", this.WORLD_VIEW_NAAG_ANUM);
    __sqoop$field_map.put("WORLD_VIEW_ANUM", this.WORLD_VIEW_ANUM);
    __sqoop$field_map.put("WORLD_VIEW_NAAG_BNUM", this.WORLD_VIEW_NAAG_BNUM);
    __sqoop$field_map.put("WORLD_VIEW_BNUM", this.WORLD_VIEW_BNUM);
    __sqoop$field_map.put("TRAFFIC_BASE_DIRECTION", this.TRAFFIC_BASE_DIRECTION);
    __sqoop$field_map.put("DATA_UNIT", this.DATA_UNIT);
    __sqoop$field_map.put("DATA_UNIT_2", this.DATA_UNIT_2);
    __sqoop$field_map.put("DATA_UNIT_3", this.DATA_UNIT_3);
    __sqoop$field_map.put("DATA_UNIT_4", this.DATA_UNIT_4);
    __sqoop$field_map.put("DISCRETE_RATING_PARAMETER_1", this.DISCRETE_RATING_PARAMETER_1);
    __sqoop$field_map.put("DISCRETE_RATING_PARAMETER_2", this.DISCRETE_RATING_PARAMETER_2);
    __sqoop$field_map.put("DISCRETE_RATING_PARAMETER_3", this.DISCRETE_RATING_PARAMETER_3);
    __sqoop$field_map.put("REVENUE_SHARE_CURRENCY_1", this.REVENUE_SHARE_CURRENCY_1);
    __sqoop$field_map.put("REVENUE_SHARE_CURRENCY_2", this.REVENUE_SHARE_CURRENCY_2);
    __sqoop$field_map.put("REVENUE_SHARE_CURRENCY_3", this.REVENUE_SHARE_CURRENCY_3);
    __sqoop$field_map.put("ANUM_OPERATOR", this.ANUM_OPERATOR);
    __sqoop$field_map.put("ANUM_CNP", this.ANUM_CNP);
    __sqoop$field_map.put("TRAFFIC_NAAG", this.TRAFFIC_NAAG);
    __sqoop$field_map.put("NAAG_ANUM_LEVEL", this.NAAG_ANUM_LEVEL);
    __sqoop$field_map.put("RECON_NAAG_ANUM", this.RECON_NAAG_ANUM);
    __sqoop$field_map.put("NETWORK_ADDRESS_AGGR_ANUM", this.NETWORK_ADDRESS_AGGR_ANUM);
    __sqoop$field_map.put("NETWORK_TYPE_ANUM", this.NETWORK_TYPE_ANUM);
    __sqoop$field_map.put("BNUM_OPERATOR", this.BNUM_OPERATOR);
    __sqoop$field_map.put("BNUM_CNP", this.BNUM_CNP);
    __sqoop$field_map.put("NAAG_BNUM_LEVEL", this.NAAG_BNUM_LEVEL);
    __sqoop$field_map.put("NETWORK_TYPE_BNUM", this.NETWORK_TYPE_BNUM);
    __sqoop$field_map.put("RECON_NAAG_BNUM", this.RECON_NAAG_BNUM);
    __sqoop$field_map.put("NETWORK_ADDRESS_AGGR_BNUM", this.NETWORK_ADDRESS_AGGR_BNUM);
    __sqoop$field_map.put("DERIVED_PRODUCT_INDICATOR", this.DERIVED_PRODUCT_INDICATOR);
    __sqoop$field_map.put("ANUM", this.ANUM);
    __sqoop$field_map.put("BNUM", this.BNUM);
    __sqoop$field_map.put("RECORD_SEQUENCE_NUMBER", this.RECORD_SEQUENCE_NUMBER);
    __sqoop$field_map.put("USER_DATA", this.USER_DATA);
    __sqoop$field_map.put("USER_DATA_2", this.USER_DATA_2);
    __sqoop$field_map.put("USER_DATA_3", this.USER_DATA_3);
    __sqoop$field_map.put("CALL_COUNT", this.CALL_COUNT);
    __sqoop$field_map.put("START_CALL_COUNT", this.START_CALL_COUNT);
    __sqoop$field_map.put("RATE_STEP_CALL_COUNT", this.RATE_STEP_CALL_COUNT);
    __sqoop$field_map.put("APPORTIONED_CALL_COUNT", this.APPORTIONED_CALL_COUNT);
    __sqoop$field_map.put("APPORTIONED_DURATION_SECONDS", this.APPORTIONED_DURATION_SECONDS);
    __sqoop$field_map.put("ACTUAL_USAGE", this.ACTUAL_USAGE);
    __sqoop$field_map.put("CHARGED_USAGE", this.CHARGED_USAGE);
    __sqoop$field_map.put("CHARGED_UNITS", this.CHARGED_UNITS);
    __sqoop$field_map.put("EVENT_DURATION", this.EVENT_DURATION);
    __sqoop$field_map.put("MINIMUM_CHARGE_ADJUSTMENT", this.MINIMUM_CHARGE_ADJUSTMENT);
    __sqoop$field_map.put("MAXIMUM_CHARGE_ADJUSTMENT", this.MAXIMUM_CHARGE_ADJUSTMENT);
    __sqoop$field_map.put("NETWORK_DURATION", this.NETWORK_DURATION);
    __sqoop$field_map.put("DATA_VOLUME", this.DATA_VOLUME);
    __sqoop$field_map.put("DATA_VOLUME_2", this.DATA_VOLUME_2);
    __sqoop$field_map.put("DATA_VOLUME_3", this.DATA_VOLUME_3);
    __sqoop$field_map.put("DATA_VOLUME_4", this.DATA_VOLUME_4);
    __sqoop$field_map.put("REVENUE_SHARE_AMOUNT_1", this.REVENUE_SHARE_AMOUNT_1);
    __sqoop$field_map.put("REVENUE_SHARE_AMOUNT_2", this.REVENUE_SHARE_AMOUNT_2);
    __sqoop$field_map.put("REVENUE_SHARE_AMOUNT_3", this.REVENUE_SHARE_AMOUNT_3);
    __sqoop$field_map.put("BASE_AMOUNT", this.BASE_AMOUNT);
    __sqoop$field_map.put("AMOUNT", this.AMOUNT);
    __sqoop$field_map.put("DLYS_DETAIL_ID", this.DLYS_DETAIL_ID);
    __sqoop$field_map.put("TRAFFIC_PERIOD", this.TRAFFIC_PERIOD);
    __sqoop$field_map.put("MESSAGE_DATE", this.MESSAGE_DATE);
    __sqoop$field_map.put("BILLING_DATE", this.BILLING_DATE);
    __sqoop$field_map.put("ADJUSTED_DATE", this.ADJUSTED_DATE);
    __sqoop$field_map.put("PROCESS_DATE", this.PROCESS_DATE);
    __sqoop$field_map.put("EVENT_START_DATE", this.EVENT_START_DATE);
    __sqoop$field_map.put("EVENT_START_TIME", this.EVENT_START_TIME);
    __sqoop$field_map.put("NETWORK_START_DATE", this.NETWORK_START_DATE);
    __sqoop$field_map.put("NETWORK_START_TIME", this.NETWORK_START_TIME);
    __sqoop$field_map.put("BILLING_START_TIME", this.BILLING_START_TIME);
    __sqoop$field_map.put("BILLING_END_TIME", this.BILLING_END_TIME);
    __sqoop$field_map.put("FLAT_RATE_CHARGE", this.FLAT_RATE_CHARGE);
    __sqoop$field_map.put("RATE_STEP_FLAT_CHARGE", this.RATE_STEP_FLAT_CHARGE);
    __sqoop$field_map.put("UNIT_COST_USED", this.UNIT_COST_USED);
    __sqoop$field_map.put("CHARGE_BUNDLE", this.CHARGE_BUNDLE);
    __sqoop$field_map.put("BASE_AMOUNT_FACTOR_USED", this.BASE_AMOUNT_FACTOR_USED);
    __sqoop$field_map.put("REFUND_FACTOR", this.REFUND_FACTOR);
    __sqoop$field_map.put("CURRENCY", this.CURRENCY);
    __sqoop$field_map.put("CURRENCY_CONVERSION_RATE", this.CURRENCY_CONVERSION_RATE);
    __sqoop$field_map.put("BASE_UNIT", this.BASE_UNIT);
    __sqoop$field_map.put("RATE_UNIT", this.RATE_UNIT);
    __sqoop$field_map.put("ROUNDED_UNIT_ID", this.ROUNDED_UNIT_ID);
    __sqoop$field_map.put("RECORD_TYPE", this.RECORD_TYPE);
    __sqoop$field_map.put("LINK_FIELD", this.LINK_FIELD);
    __sqoop$field_map.put("REASON_FOR_CLEARDOWN", this.REASON_FOR_CLEARDOWN);
    __sqoop$field_map.put("REPAIR_INDICATOR", this.REPAIR_INDICATOR);
    __sqoop$field_map.put("RATED_BILLING_PERIOD", this.RATED_BILLING_PERIOD);
    __sqoop$field_map.put("TRAFFIC_MOVEMENT_CTR", this.TRAFFIC_MOVEMENT_CTR);
    __sqoop$field_map.put("RTE_LOOKUP_STYLE", this.RTE_LOOKUP_STYLE);
    __sqoop$field_map.put("CDR_SOURCE", this.CDR_SOURCE);
    __sqoop$field_map.put("CARRIER_DESTINATION_NAME", this.CARRIER_DESTINATION_NAME);
    __sqoop$field_map.put("CARRIER_DESTINATION_ALIAS", this.CARRIER_DESTINATION_ALIAS);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("CDR_FILE_NO", this.CDR_FILE_NO);
    __sqoop$field_map.put("CDR_UCI_NO", this.CDR_UCI_NO);
    __sqoop$field_map.put("CDR_UCI_ELEMENT", this.CDR_UCI_ELEMENT);
    __sqoop$field_map.put("CDR_KEY_ID", this.CDR_KEY_ID);
    __sqoop$field_map.put("CALL_SCENARIO", this.CALL_SCENARIO);
    __sqoop$field_map.put("REMUNERATION_INDICATOR", this.REMUNERATION_INDICATOR);
    __sqoop$field_map.put("BILLING_BASE_DIRECTION", this.BILLING_BASE_DIRECTION);
    __sqoop$field_map.put("FRANCHISE", this.FRANCHISE);
    __sqoop$field_map.put("PRODUCT_GROUP", this.PRODUCT_GROUP);
    __sqoop$field_map.put("REVERSE_INDICATOR", this.REVERSE_INDICATOR);
    __sqoop$field_map.put("EVENT_DIRECTION", this.EVENT_DIRECTION);
    __sqoop$field_map.put("DERIVED_EVENT_DIRECTION", this.DERIVED_EVENT_DIRECTION);
    __sqoop$field_map.put("BILLING_RATING_SCENARIO_TYPE", this.BILLING_RATING_SCENARIO_TYPE);
    __sqoop$field_map.put("BILLED_PRODUCT", this.BILLED_PRODUCT);
    __sqoop$field_map.put("RATING_SCENARIO", this.RATING_SCENARIO);
    __sqoop$field_map.put("RATING_RULE_DEPENDENCY_IND", this.RATING_RULE_DEPENDENCY_IND);
    __sqoop$field_map.put("ESTIMATE_INDICATOR", this.ESTIMATE_INDICATOR);
    __sqoop$field_map.put("ACCOUNTING_METHOD", this.ACCOUNTING_METHOD);
    __sqoop$field_map.put("CASH_FLOW", this.CASH_FLOW);
    __sqoop$field_map.put("COMPONENT_DIRECTION", this.COMPONENT_DIRECTION);
    __sqoop$field_map.put("STATEMENT_DIRECTION", this.STATEMENT_DIRECTION);
    __sqoop$field_map.put("BILLING_METHOD", this.BILLING_METHOD);
    __sqoop$field_map.put("RATING_COMPONENT", this.RATING_COMPONENT);
    __sqoop$field_map.put("CALL_ATTEMPT_INDICATOR", this.CALL_ATTEMPT_INDICATOR);
    __sqoop$field_map.put("PARAMETER_MATRIX", this.PARAMETER_MATRIX);
    __sqoop$field_map.put("PARAMETER_MATRIX_LINE", this.PARAMETER_MATRIX_LINE);
    __sqoop$field_map.put("STEP_NUMBER", this.STEP_NUMBER);
    __sqoop$field_map.put("RATE_TYPE", this.RATE_TYPE);
    __sqoop$field_map.put("TIME_PREMIUM", this.TIME_PREMIUM);
    __sqoop$field_map.put("BILLING_OPERATOR", this.BILLING_OPERATOR);
    __sqoop$field_map.put("RATE_OWNER_OPERATOR", this.RATE_OWNER_OPERATOR);
    __sqoop$field_map.put("SETTLEMENT_OPERATOR", this.SETTLEMENT_OPERATOR);
    __sqoop$field_map.put("RATE_NAME", this.RATE_NAME);
    __sqoop$field_map.put("TIER", this.TIER);
    __sqoop$field_map.put("TIER_GROUP", this.TIER_GROUP);
    __sqoop$field_map.put("TIER_TYPE", this.TIER_TYPE);
    __sqoop$field_map.put("OPTIMUM_POI", this.OPTIMUM_POI);
    __sqoop$field_map.put("ADHOC_SUMMARY_IND", this.ADHOC_SUMMARY_IND);
    __sqoop$field_map.put("BILLING_SUMMARY_IND", this.BILLING_SUMMARY_IND);
    __sqoop$field_map.put("COMPLEMENTARY_SUMMARY_IND", this.COMPLEMENTARY_SUMMARY_IND);
    __sqoop$field_map.put("TRAFFIC_SUMMARY_IND", this.TRAFFIC_SUMMARY_IND);
    __sqoop$field_map.put("MINIMUM_CHARGE_IND", this.MINIMUM_CHARGE_IND);
    __sqoop$field_map.put("CALL_DURATION_BUCKET", this.CALL_DURATION_BUCKET);
    __sqoop$field_map.put("TIME_OF_DAY_BUCKET", this.TIME_OF_DAY_BUCKET);
    __sqoop$field_map.put("USER_SUMMARISATION", this.USER_SUMMARISATION);
    __sqoop$field_map.put("INCOMING_NODE", this.INCOMING_NODE);
    __sqoop$field_map.put("INCOMING_PRODUCT", this.INCOMING_PRODUCT);
    __sqoop$field_map.put("INCOMING_OPERATOR", this.INCOMING_OPERATOR);
    __sqoop$field_map.put("INCOMING_PATH", this.INCOMING_PATH);
    __sqoop$field_map.put("INCOMING_POI", this.INCOMING_POI);
    __sqoop$field_map.put("OUTGOING_NODE", this.OUTGOING_NODE);
    __sqoop$field_map.put("OUTGOING_PRODUCT", this.OUTGOING_PRODUCT);
    __sqoop$field_map.put("OUTGOING_OPERATOR", this.OUTGOING_OPERATOR);
    __sqoop$field_map.put("OUTGOING_PATH", this.OUTGOING_PATH);
    __sqoop$field_map.put("OUTGOING_POI", this.OUTGOING_POI);
    __sqoop$field_map.put("TRAFFIC_RATING_SCENARIO_TYPE", this.TRAFFIC_RATING_SCENARIO_TYPE);
    __sqoop$field_map.put("TRAFFIC_ROUTE_TYPE", this.TRAFFIC_ROUTE_TYPE);
    __sqoop$field_map.put("TRAFFIC_ROUTE", this.TRAFFIC_ROUTE);
    __sqoop$field_map.put("TRAFFIC_AGREEMENT_OPERATOR", this.TRAFFIC_AGREEMENT_OPERATOR);
    __sqoop$field_map.put("TRAFFIC_NEGOTIATION_DIR", this.TRAFFIC_NEGOTIATION_DIR);
    __sqoop$field_map.put("BILLING_ROUTE_TYPE", this.BILLING_ROUTE_TYPE);
    __sqoop$field_map.put("BILLING_ROUTE", this.BILLING_ROUTE);
    __sqoop$field_map.put("BILLING_AGREEMENT_OPERATOR", this.BILLING_AGREEMENT_OPERATOR);
    __sqoop$field_map.put("BILLING_NEGOTIATION_DIR", this.BILLING_NEGOTIATION_DIR);
    __sqoop$field_map.put("ROUTE_IDENTIFIER", this.ROUTE_IDENTIFIER);
    __sqoop$field_map.put("REFILE_INDICATOR", this.REFILE_INDICATOR);
    __sqoop$field_map.put("AGREEMENT_NAAG_ANUM_LEVEL", this.AGREEMENT_NAAG_ANUM_LEVEL);
    __sqoop$field_map.put("AGREEMENT_NAAG_ANUM", this.AGREEMENT_NAAG_ANUM);
    __sqoop$field_map.put("AGREEMENT_NAAG_BNUM_LEVEL", this.AGREEMENT_NAAG_BNUM_LEVEL);
    __sqoop$field_map.put("AGREEMENT_NAAG_BNUM", this.AGREEMENT_NAAG_BNUM);
    __sqoop$field_map.put("WORLD_VIEW_NAAG_ANUM", this.WORLD_VIEW_NAAG_ANUM);
    __sqoop$field_map.put("WORLD_VIEW_ANUM", this.WORLD_VIEW_ANUM);
    __sqoop$field_map.put("WORLD_VIEW_NAAG_BNUM", this.WORLD_VIEW_NAAG_BNUM);
    __sqoop$field_map.put("WORLD_VIEW_BNUM", this.WORLD_VIEW_BNUM);
    __sqoop$field_map.put("TRAFFIC_BASE_DIRECTION", this.TRAFFIC_BASE_DIRECTION);
    __sqoop$field_map.put("DATA_UNIT", this.DATA_UNIT);
    __sqoop$field_map.put("DATA_UNIT_2", this.DATA_UNIT_2);
    __sqoop$field_map.put("DATA_UNIT_3", this.DATA_UNIT_3);
    __sqoop$field_map.put("DATA_UNIT_4", this.DATA_UNIT_4);
    __sqoop$field_map.put("DISCRETE_RATING_PARAMETER_1", this.DISCRETE_RATING_PARAMETER_1);
    __sqoop$field_map.put("DISCRETE_RATING_PARAMETER_2", this.DISCRETE_RATING_PARAMETER_2);
    __sqoop$field_map.put("DISCRETE_RATING_PARAMETER_3", this.DISCRETE_RATING_PARAMETER_3);
    __sqoop$field_map.put("REVENUE_SHARE_CURRENCY_1", this.REVENUE_SHARE_CURRENCY_1);
    __sqoop$field_map.put("REVENUE_SHARE_CURRENCY_2", this.REVENUE_SHARE_CURRENCY_2);
    __sqoop$field_map.put("REVENUE_SHARE_CURRENCY_3", this.REVENUE_SHARE_CURRENCY_3);
    __sqoop$field_map.put("ANUM_OPERATOR", this.ANUM_OPERATOR);
    __sqoop$field_map.put("ANUM_CNP", this.ANUM_CNP);
    __sqoop$field_map.put("TRAFFIC_NAAG", this.TRAFFIC_NAAG);
    __sqoop$field_map.put("NAAG_ANUM_LEVEL", this.NAAG_ANUM_LEVEL);
    __sqoop$field_map.put("RECON_NAAG_ANUM", this.RECON_NAAG_ANUM);
    __sqoop$field_map.put("NETWORK_ADDRESS_AGGR_ANUM", this.NETWORK_ADDRESS_AGGR_ANUM);
    __sqoop$field_map.put("NETWORK_TYPE_ANUM", this.NETWORK_TYPE_ANUM);
    __sqoop$field_map.put("BNUM_OPERATOR", this.BNUM_OPERATOR);
    __sqoop$field_map.put("BNUM_CNP", this.BNUM_CNP);
    __sqoop$field_map.put("NAAG_BNUM_LEVEL", this.NAAG_BNUM_LEVEL);
    __sqoop$field_map.put("NETWORK_TYPE_BNUM", this.NETWORK_TYPE_BNUM);
    __sqoop$field_map.put("RECON_NAAG_BNUM", this.RECON_NAAG_BNUM);
    __sqoop$field_map.put("NETWORK_ADDRESS_AGGR_BNUM", this.NETWORK_ADDRESS_AGGR_BNUM);
    __sqoop$field_map.put("DERIVED_PRODUCT_INDICATOR", this.DERIVED_PRODUCT_INDICATOR);
    __sqoop$field_map.put("ANUM", this.ANUM);
    __sqoop$field_map.put("BNUM", this.BNUM);
    __sqoop$field_map.put("RECORD_SEQUENCE_NUMBER", this.RECORD_SEQUENCE_NUMBER);
    __sqoop$field_map.put("USER_DATA", this.USER_DATA);
    __sqoop$field_map.put("USER_DATA_2", this.USER_DATA_2);
    __sqoop$field_map.put("USER_DATA_3", this.USER_DATA_3);
    __sqoop$field_map.put("CALL_COUNT", this.CALL_COUNT);
    __sqoop$field_map.put("START_CALL_COUNT", this.START_CALL_COUNT);
    __sqoop$field_map.put("RATE_STEP_CALL_COUNT", this.RATE_STEP_CALL_COUNT);
    __sqoop$field_map.put("APPORTIONED_CALL_COUNT", this.APPORTIONED_CALL_COUNT);
    __sqoop$field_map.put("APPORTIONED_DURATION_SECONDS", this.APPORTIONED_DURATION_SECONDS);
    __sqoop$field_map.put("ACTUAL_USAGE", this.ACTUAL_USAGE);
    __sqoop$field_map.put("CHARGED_USAGE", this.CHARGED_USAGE);
    __sqoop$field_map.put("CHARGED_UNITS", this.CHARGED_UNITS);
    __sqoop$field_map.put("EVENT_DURATION", this.EVENT_DURATION);
    __sqoop$field_map.put("MINIMUM_CHARGE_ADJUSTMENT", this.MINIMUM_CHARGE_ADJUSTMENT);
    __sqoop$field_map.put("MAXIMUM_CHARGE_ADJUSTMENT", this.MAXIMUM_CHARGE_ADJUSTMENT);
    __sqoop$field_map.put("NETWORK_DURATION", this.NETWORK_DURATION);
    __sqoop$field_map.put("DATA_VOLUME", this.DATA_VOLUME);
    __sqoop$field_map.put("DATA_VOLUME_2", this.DATA_VOLUME_2);
    __sqoop$field_map.put("DATA_VOLUME_3", this.DATA_VOLUME_3);
    __sqoop$field_map.put("DATA_VOLUME_4", this.DATA_VOLUME_4);
    __sqoop$field_map.put("REVENUE_SHARE_AMOUNT_1", this.REVENUE_SHARE_AMOUNT_1);
    __sqoop$field_map.put("REVENUE_SHARE_AMOUNT_2", this.REVENUE_SHARE_AMOUNT_2);
    __sqoop$field_map.put("REVENUE_SHARE_AMOUNT_3", this.REVENUE_SHARE_AMOUNT_3);
    __sqoop$field_map.put("BASE_AMOUNT", this.BASE_AMOUNT);
    __sqoop$field_map.put("AMOUNT", this.AMOUNT);
    __sqoop$field_map.put("DLYS_DETAIL_ID", this.DLYS_DETAIL_ID);
    __sqoop$field_map.put("TRAFFIC_PERIOD", this.TRAFFIC_PERIOD);
    __sqoop$field_map.put("MESSAGE_DATE", this.MESSAGE_DATE);
    __sqoop$field_map.put("BILLING_DATE", this.BILLING_DATE);
    __sqoop$field_map.put("ADJUSTED_DATE", this.ADJUSTED_DATE);
    __sqoop$field_map.put("PROCESS_DATE", this.PROCESS_DATE);
    __sqoop$field_map.put("EVENT_START_DATE", this.EVENT_START_DATE);
    __sqoop$field_map.put("EVENT_START_TIME", this.EVENT_START_TIME);
    __sqoop$field_map.put("NETWORK_START_DATE", this.NETWORK_START_DATE);
    __sqoop$field_map.put("NETWORK_START_TIME", this.NETWORK_START_TIME);
    __sqoop$field_map.put("BILLING_START_TIME", this.BILLING_START_TIME);
    __sqoop$field_map.put("BILLING_END_TIME", this.BILLING_END_TIME);
    __sqoop$field_map.put("FLAT_RATE_CHARGE", this.FLAT_RATE_CHARGE);
    __sqoop$field_map.put("RATE_STEP_FLAT_CHARGE", this.RATE_STEP_FLAT_CHARGE);
    __sqoop$field_map.put("UNIT_COST_USED", this.UNIT_COST_USED);
    __sqoop$field_map.put("CHARGE_BUNDLE", this.CHARGE_BUNDLE);
    __sqoop$field_map.put("BASE_AMOUNT_FACTOR_USED", this.BASE_AMOUNT_FACTOR_USED);
    __sqoop$field_map.put("REFUND_FACTOR", this.REFUND_FACTOR);
    __sqoop$field_map.put("CURRENCY", this.CURRENCY);
    __sqoop$field_map.put("CURRENCY_CONVERSION_RATE", this.CURRENCY_CONVERSION_RATE);
    __sqoop$field_map.put("BASE_UNIT", this.BASE_UNIT);
    __sqoop$field_map.put("RATE_UNIT", this.RATE_UNIT);
    __sqoop$field_map.put("ROUNDED_UNIT_ID", this.ROUNDED_UNIT_ID);
    __sqoop$field_map.put("RECORD_TYPE", this.RECORD_TYPE);
    __sqoop$field_map.put("LINK_FIELD", this.LINK_FIELD);
    __sqoop$field_map.put("REASON_FOR_CLEARDOWN", this.REASON_FOR_CLEARDOWN);
    __sqoop$field_map.put("REPAIR_INDICATOR", this.REPAIR_INDICATOR);
    __sqoop$field_map.put("RATED_BILLING_PERIOD", this.RATED_BILLING_PERIOD);
    __sqoop$field_map.put("TRAFFIC_MOVEMENT_CTR", this.TRAFFIC_MOVEMENT_CTR);
    __sqoop$field_map.put("RTE_LOOKUP_STYLE", this.RTE_LOOKUP_STYLE);
    __sqoop$field_map.put("CDR_SOURCE", this.CDR_SOURCE);
    __sqoop$field_map.put("CARRIER_DESTINATION_NAME", this.CARRIER_DESTINATION_NAME);
    __sqoop$field_map.put("CARRIER_DESTINATION_ALIAS", this.CARRIER_DESTINATION_ALIAS);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
