package cosmos.results;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Writable;

public interface QueryResult<T> extends Writable {
  public String docId();
  
  public String document();
  
  public T typedDocument();
  
  public ColumnVisibility documentVisibility();
  
  public Iterable<Entry<Column,SValue>> columnValues();
  
  public Value toValue() throws IOException;
}
