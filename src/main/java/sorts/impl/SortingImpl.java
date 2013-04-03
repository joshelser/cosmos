package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sorts.SortableResult;
import sorts.Sorting;
import sorts.SortingMetadata;
import sorts.SortingMetadata.State;
import sorts.UnexpectedStateException;
import sorts.options.Index;
import sorts.options.Order;
import sorts.options.Ordering;
import sorts.options.Paging;
import sorts.results.Column;
import sorts.results.PagedQueryResult;
import sorts.results.QueryResult;
import sorts.results.Value;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class SortingImpl implements Sorting {
  private static final Logger log = LoggerFactory.getLogger(SortingImpl.class);
  
  public static final String NULL_BYTE_STR = "\0";
  public static final String DOCID_FIELD_NAME = "SORTS_DOCID";
  public static final String FORWARD = "f";
  public static final String REVERSE = "r";
  
  private final BatchWriterConfig DEFAULT_BW_CONFIG = new BatchWriterConfig();
  
  public void register(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.UNKNOWN.equals(s)) {
      UnexpectedStateException e = unexpectedState(id, State.UNKNOWN, s);
      log.error(e.getMessage());
      throw e;
    }
    
    State targetState = State.LOADING;
    
    log.debug("Setting state for {} from {} to {}", new Object[] {id, s, targetState});
    
    SortingMetadata.setState(id, targetState);
  }
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults) throws TableNotFoundException, MutationsRejectedException,
      UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(queryResults);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s)) {
      UnexpectedStateException e = unexpectedState(id, State.LOADING, s);
      log.error(e.getMessage());
      throw e;
    }
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
      
      for (QueryResult<?> result : queryResults) {
        bw.addMutation(addDocument(id, result));
      }
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
  }
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults, Iterable<Entry<Column,Index>> columnsToIndex) throws TableNotFoundException,
      MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(queryResults);
    checkNotNull(columnsToIndex);
    
    addResults(id, queryResults);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s)) {
      UnexpectedStateException e = unexpectedState(id, State.LOADING, s);
      log.error(e.getMessage());
      throw e;
    }
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
      
      final Multimap<Column,Index> columns = HashMultimap.create();
      
      for (Entry<Column,Index> column : columnsToIndex) {
        columns.put(column.getKey(), column.getValue());
      }
      
      for (QueryResult<?> result : queryResults) {
        for (Entry<Column,Value> entry : result.columnValues()) {
          final Column c = entry.getKey();
          final Value v = entry.getValue();
          
          if (columns.containsKey(c)) {
            for (Index index : columns.get(c)) {
              Mutation m = getDocumentPrefix(id, result, v.value());
              
              final String direction = Order.ASCENDING.equals(index.order()) ? FORWARD : REVERSE;
              m.put(index.column(), direction + NULL_BYTE_STR + result.docId(), result.documentVisibility(), new org.apache.accumulo.core.data.Value(result
                  .document().getBytes()));
              
              bw.addMutation(m);
            }
          }
        }
      }
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
  }
  
  public void finalize(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s)) {
      throw unexpectedState(id, State.LOADING, s);
    }
    
    final State desiredState = State.LOADED;
    
    log.debug("Changing state for {} from {} to {}", new Object[] {id, s, desiredState});
    
    SortingMetadata.setState(id, desiredState);
  }
  
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
  
  public void delete(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADED.equals(s)) {
      throw unexpectedState(id, State.LOADED, s);
    }
    
    final State desiredState = State.DELETING;

    log.debug("Changing state for {} from {} to {}", new Object[] {id, s, desiredState});
    
    SortingMetadata.setState(id, desiredState);
    
    // TODO implement deletion of the Keys
    
    log.debug("Removing state for {}", id);
    
    SortingMetadata.remove(id);
  }
  
  protected Mutation getDocumentPrefix(SortableResult id, QueryResult<?> queryResult, String suffix) {
    return new Mutation(id.uuid() + NULL_BYTE_STR + suffix);
  }
  
  protected Mutation addDocument(SortableResult id, QueryResult<?> queryResult) {
    Mutation m = getDocumentPrefix(id, queryResult, queryResult.docId());
    
    m.put(DOCID_FIELD_NAME, FORWARD + NULL_BYTE_STR + queryResult.docId(), queryResult.documentVisibility(), new org.apache.accumulo.core.data.Value(
        queryResult.document().getBytes()));
    
    return m;
  }
  
  protected UnexpectedStateException unexpectedState(SortableResult id, State expected, State actual) {
    return new UnexpectedStateException("Invalid state " + id + " for " + id + ". Expected " + actual + " but was " + actual);
  }
  
}
