package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
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
import sorts.results.SValue;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

public class SortingImpl implements Sorting {
  private static final Logger log = LoggerFactory.getLogger(SortingImpl.class);
  
  public static final String NULL_BYTE_STR = "\0";
  public static final String DOCID_FIELD_NAME = "SORTS_DOCID";
  public static final Text DOCID_FIELD_NAME_TEXT = new Text(DOCID_FIELD_NAME);
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
      UnexpectedStateException, IOException {
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
  
  public void addResults(SortableResult id, Iterable<QueryResult<?>> queryResults, Iterable<Index> columnsToIndex) throws TableNotFoundException,
      MutationsRejectedException, UnexpectedStateException, IOException {
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
      
      final Multimap<Column,Index> columns = mapForIndexedColumns(columnsToIndex);

      for (QueryResult<?> result : queryResults) {
        bw.addMutation(addDocument(id, result));
        
        for (Entry<Column,SValue> entry : result.columnValues()) {
          final Column c = entry.getKey();
          final SValue v = entry.getValue();
          
          if (columns.containsKey(c)) {
            for (Index index : columns.get(c)) {
              Mutation m = getDocumentPrefix(id, result, v.value());
              
              final String direction = Order.ASCENDING.equals(index.order()) ? FORWARD : REVERSE;
              m.put(index.column().toString(), direction + NULL_BYTE_STR + result.docId(), result.documentVisibility(), result.toValue());
              
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
  
  public void index(SortableResult id, Iterable<Index> columnsToIndex) throws TableNotFoundException, UnexpectedStateException, MutationsRejectedException, IOException {
    checkNotNull(id);
    checkNotNull(columnsToIndex);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s)) {
      throw unexpectedState(id, State.LOADING, s);
    }
    
    final Multimap<Column,Index> columns = mapForIndexedColumns(columnsToIndex);
    final int numCols = columns.keySet().size();
    
    Iterable<MultimapQueryResult> results = fetch(id);
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
    
      for (MultimapQueryResult result : results) {
        if (result.columnSize() > numCols) {
          for (Entry<Column,Index> entry : columns.entries()) {
            if (result.containsKey(entry.getKey())) {
              Collection<SValue> values = result.get(entry.getKey());
              for (SValue value :  values) {
                Mutation m = getDocumentPrefix(id, result, value.value());
                
                final String direction = Order.ASCENDING.equals(entry.getValue().order()) ? FORWARD : REVERSE;
                m.put(entry.getValue().column().toString(), direction + NULL_BYTE_STR + result.docId(), result.documentVisibility(), result.toValue());
                
                bw.addMutation(m);
              }
            }
          }
        } else {
          for (Entry<Column,SValue> entry : result.columnValues()) {
            if (columns.containsKey(entry.getKey())) {
              final Collection<Index> indexes = columns.get(entry.getKey());
              for (Index index : indexes) {
                final Collection<SValue> svalues = result.get(index.column());
                for (SValue value : svalues) {
                  Mutation m = getDocumentPrefix(id, result, value.value());
                  
                  final String direction = Order.ASCENDING.equals(index.order()) ? FORWARD : REVERSE;
                  m.put(index.column().toString(), direction + NULL_BYTE_STR + result.docId(), result.documentVisibility(), result.toValue());
                  
                  bw.addMutation(m);
                }
              }
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
  
  public Iterable<Column> columns(SortableResult id) {
    return null;
  }
  
  public Iterable<MultimapQueryResult> fetch(SortableResult id) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    Scanner scanner = id.connector().createScanner(id.dataTable(), id.auths());
    scanner.setRange(Range.prefix(id.uuid()));
    scanner.fetchColumnFamily(DOCID_FIELD_NAME_TEXT);
    
    return Iterables.transform(scanner, new KVToMultimap());
  }
  
  public PagedQueryResult fetch(SortableResult id, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Ordering ordering) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Ordering ordering, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<SValue,Long>> groupResults(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<SValue,Long>> groupResults(SortableResult id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<SValue,Long>> groupResults(SortableResult id, Ordering order) throws TableNotFoundException, UnexpectedStateException {
    return null;
  }
  
  public Iterable<Entry<SValue,Long>> groupResults(SortableResult id, Ordering order, Paging limits) throws TableNotFoundException, UnexpectedStateException {
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
    
    // Delete of the Keys
    BatchDeleter bd = null;
    try {
      bd = id.connector().createBatchDeleter(id.dataTable(), id.auths(), 4, new BatchWriterConfig());
      bd.setRanges(Collections.singleton(Range.prefix(id.uuid())));
      
      bd.delete();
    } finally {
      if (null != bd) {
        bd.close();      
      }
    }
    
    log.debug("Removing state for {}", id);
    
    SortingMetadata.remove(id);
  }
  
  protected Mutation getDocumentPrefix(SortableResult id, QueryResult<?> queryResult, String suffix) {
    return new Mutation(id.uuid() + NULL_BYTE_STR + suffix);
  }
  
  protected Mutation addDocument(SortableResult id, QueryResult<?> queryResult) throws IOException {
    Mutation m = getDocumentPrefix(id, queryResult, queryResult.docId());
    
    m.put(DOCID_FIELD_NAME, FORWARD + NULL_BYTE_STR + queryResult.docId(), queryResult.documentVisibility(), queryResult.toValue());
    
    return m;
  }
  
  protected UnexpectedStateException unexpectedState(SortableResult id, State[] expected, State actual) {
    return new UnexpectedStateException("Invalid state " + id + " for " + id + ". Expected one of " + Arrays.asList(expected) + " but was " + actual);
  }
  
  protected UnexpectedStateException unexpectedState(SortableResult id, State expected, State actual) {
    return new UnexpectedStateException("Invalid state " + id + " for " + id + ". Expected " + expected + " but was " + actual);
  }
 
  protected HashMultimap<Column,Index> mapForIndexedColumns(Iterable<Index> columnsToIndex) {
    final HashMultimap<Column,Index> columns = HashMultimap.create();
    
    for (Index index : columnsToIndex) {
      columns.put(index.column(), index);
    }
    
    return columns;
  }
}
