package sorts.options;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public abstract class Defaults {
  public static final boolean LOCK_ON_UPDATES = false;
  public static final String DATA_TABLE = "sorts";
  public static final String METADATA_TABLE = "metasorts";
  
  public static final String NULL_BYTE_STR = "\u0000";
  public static final String EIN_BYTE_STR = "\u0001";
  
  public static final String DOCID_FIELD_NAME = "SORTS_DOCID";
  public static final Text DOCID_FIELD_NAME_TEXT = new Text(DOCID_FIELD_NAME);
  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  public static final String CURATOR_PREFIX = "/sorts/";
}
