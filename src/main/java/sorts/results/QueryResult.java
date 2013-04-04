package sorts.results;

import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;

public interface QueryResult<T> {
  public String docId();
  
  public String document();
  
  public T typedDocument();
  
  public ColumnVisibility documentVisibility();
  
  public Iterable<Entry<Column,SValue>> columnValues();
}
