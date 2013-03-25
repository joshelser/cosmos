package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import sorts.UnexpectedStateException;
import sorts.SortableResult;
import sorts.Sorting;
import sorts.SortingMetadata;
import sorts.SortingMetadata.State;
import sorts.options.Index;
import sorts.options.Ordering;
import sorts.options.Paging;
import sorts.results.Column;
import sorts.results.PagedQueryResult;
import sorts.results.QueryResult;
import sorts.results.Value;

public class SortingImpl implements Sorting {
  
  public void register(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.UNKNOWN.equals(s)) {
      throw new UnexpectedStateException("Results already exist for " + id);
    }
    
    SortingMetadata.setState(id, State.LOADING);
  }
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults) throws TableNotFoundException, MutationsRejectedException,
      UnexpectedStateException {}
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults, Iterable<Index> columnsToIndex) throws TableNotFoundException,
      MutationsRejectedException, UnexpectedStateException {}
  
  public void index(SortableResult id, Iterable<Column> columns) throws TableNotFoundException, UnexpectedStateException {}
  
  public Iterable<Column> columns(SortableResult id) {
    return null;
  }
  
  public Iterable<QueryResult<?>> fetch(SortableResult id) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public PagedQueryResult fetch(SortableResult id, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<QueryResult<?>> fetch(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<QueryResult<?>> fetch(SortableResult id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<QueryResult<?>> fetch(SortableResult id, Ordering ordering) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<QueryResult<?>> fetch(SortableResult id, Ordering ordering, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Ordering order) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<Value,Long>> groupResults(SortableResult id, Ordering order, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public void delete(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {}
  
}
