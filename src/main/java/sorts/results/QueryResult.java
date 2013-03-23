package sorts.results;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

public interface QueryResult<T> {
  public ByteBuffer docId();
  
  public ByteBuffer document();
  
  public T typedDocument();
  
  public Iterable<Entry<Column,Value>> columnValues();
}
