package sorts.impl;
import java.util.Map.Entry;

import sorts.SortableResult;
import sorts.Sorting;
import sorts.options.Column;
import sorts.options.Index;
import sorts.options.Ordering;
import sorts.options.Paging;
import sorts.results.PagedQueryResult;
import sorts.results.QueryResult;
import sorts.results.Value;



public class SortingImpl implements Sorting {
  
  public void addResults(SortableResult id, Iterable<QueryResult> queryResults) {}
  
  public void addResults(SortableResult id, Iterable<QueryResult> queryResults, Iterable<Index> columnsToIndex) {}
  
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
