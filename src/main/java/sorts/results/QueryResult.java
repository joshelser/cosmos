package sorts.results;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;

public interface QueryResult<T> {
  public ByteBuffer docId();
  
  public ByteBuffer document();
  
  public T typedDocument();
  
  public ColumnVisibility documentVisibility();
  
  public Iterable<Entry<Column,Value>> columnValues();
}
