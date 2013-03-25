package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

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

import com.google.common.collect.Sets;

public class SortingImpl implements Sorting {
  public static final String NULL_BYTE_STR = "\0";
  public static final String DOCID_FIELD_NAME = "SORTS_DOCID";
  public static final String FORWARD = "f";
  public static final String REVERSE = "r";
  
  private final BatchWriterConfig DEFAULT_BW_CONFIG = new BatchWriterConfig();
  
  public void register(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.UNKNOWN.equals(s)) {
      throw new UnexpectedStateException("Invalid state " + s + " for " + id + ". Expected " + State.LOADING);
    }
    
    SortingMetadata.setState(id, State.LOADING);
  }
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults) throws TableNotFoundException, MutationsRejectedException,
      UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(queryResults);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s)) {
      throw new UnexpectedStateException("Invalid state " + s + " for " + id + ". Expected " + State.LOADING);
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
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults, Iterable<Index> columnsToIndex) throws TableNotFoundException,
      MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(queryResults);
    checkNotNull(columnsToIndex);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s)) {
      throw new UnexpectedStateException("Invalid state " + s + " for " + id + ". Expected " + State.LOADING);
    }
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
    
      final Set<Index> columns = Sets.newHashSet(columnsToIndex);
      
      for (QueryResult<?> result : queryResults) {
        for (Entry<Column,Value> entry : result.columnValues()) {
          
          // rework this hunk-o-junk
          
          Mutation m = getDocumentPrefix(id, result);
          
          final byte[] bytes = new byte[result.document().position()];
          result.document().get(bytes);
          
          final String direction = Order.ASCENDING.equals(index.order()) ? FORWARD : REVERSE;
          m.put(index.column(), direction + NULL_BYTE_STR + result.docId(), result.documentVisibility(), 
              new org.apache.accumulo.core.data.Value(bytes));
        }
        
        bw.addMutation(m);
      }
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
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
  
  public void delete(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {}
  
  protected Mutation getDocumentPrefix(SortableResult id, QueryResult<?> queryResult, String suffix) {
    return new Mutation(id.uuid() + NULL_BYTE_STR + suffix);
  }
  
  protected Mutation addDocument(SortableResult id, QueryResult<?> queryResult) {
    final byte[] docId = new byte[queryResult.docId().position()];
    queryResult.docId().get(docId);
    final String docIdStr = new String(docId);

    Mutation m = getDocumentPrefix(id, queryResult, docIdStr);
    
    m.put(DOCID_FIELD_NAME, FORWARD + NULL_BYTE_STR + docIdStr, queryResult.documentVisibility(),
        new org.apache.accumulo.core.data.Value(queryResult.typedDocument().toString().getBytes()));
    
    return m;
  }
}
