import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;

import com.google.common.base.Preconditions;


public class SortingImpl implements Sorting {
  
  private final Connector connector;
  
  public SortingImpl(Connector connector) {
    Preconditions.checkNotNull(connector);
    
    this.connector = connector;
  }
  
  public SortableResult register() {
    return null;
  }
  
  public void addResults(SortableResult id, Iterable<QueryResult> queryResults) {}
  
  public void addResultsWithIndex(SortableResult id, Iterable<QueryResult> queryResults, Iterable<Index> columnsToIndex) {}
  
  public void index(SortableResult id, Iterable<Column> columns) {}
  
  public Iterable<Column> columns(SortableResult id) {
    return null;
  }
  
  public Iterable<QueryResult> fetch(SortableResult id) {
    return null;
  }
  
  public PagedQueryResult fetch(SortableResult id, Paging limits) {
    return null;
  }
  
  public Iterable<QueryResult> fetch(SortableResult id, Column column) {
    return null;
  }
  
  public Iterable<QueryResult> fetch(SortableResult id, Column column, Paging limits) {
    return null;
  }
  
  public Iterable<QueryResult> fetch(SortableResult id, Ordering ordering) {
    return null;
  }
  
  public Iterable<QueryResult> fetch(SortableResult id, Ordering ordering, Paging limits) {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Column column) {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Column column, Paging limits) {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Ordering order) {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Ordering order, Paging limits) {
    return null;
  }
  
  public void delete(SortableResult id) {}
  
}
