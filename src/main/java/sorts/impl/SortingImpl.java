package sorts.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sorts.Sorting;
import sorts.SortingMetadata;
import sorts.SortingMetadata.State;
import sorts.UnexpectedStateException;
import sorts.UnindexedColumnException;
import sorts.accumulo.GroupByRowSuffixIterator;
import sorts.options.Index;
import sorts.options.Order;
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
  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  
  private final BatchWriterConfig DEFAULT_BW_CONFIG = new BatchWriterConfig();
  
  @Override
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
  
  @Override
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
    
    Set<Index> columnsToIndex = id.columnsToIndex();
    
    BatchWriter bw = null, metadataBw = null;
    try {
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
      metadataBw = id.connector().createBatchWriter(id.metadataTable(), DEFAULT_BW_CONFIG);
      
      // TODO This is broken with the identity set
      final Multimap<Column,Index> columns = mapForIndexedColumns(columnsToIndex);
      final Text holder = new Text();
      
      for (QueryResult<?> result : queryResults) {
        bw.addMutation(addDocument(id, result));
        Mutation columnMutation = new Mutation(id.uuid());
        
        for (Entry<Column,SValue> entry : result.columnValues()) {
          final Column c = entry.getKey();
          final SValue v = entry.getValue();
          holder.set(c.column());
          
          columnMutation.put(SortingMetadata.COLUMN_COLFAM, holder, EMPTY_VALUE);
          
          if (columns.containsKey(c)) {
            for (Index index : columns.get(c)) {
              Mutation m = getDocumentPrefix(id, result, v.value());
              
              final String direction = Order.ASCENDING.equals(index.order()) ? FORWARD : REVERSE;
              m.put(index.column().toString(), direction + NULL_BYTE_STR + result.docId(), result.documentVisibility(), result.toValue());
              
              bw.addMutation(m);
            }
          }
        }
        
        metadataBw.addMutation(columnMutation);
      }
    } catch (MutationsRejectedException e) {
      log.error("Caught exception adding results for {}", id, e);
      throw e;
    } catch (TableNotFoundException e) {
      log.error("Caught exception adding results for {}", id, e);
      throw e;
    } catch (RuntimeException e) {
      log.error("Caught exception adding results for {}", id, e);
      throw e;
    } finally {
      if (null != bw) {
        bw.close();
      }
      if (null != metadataBw) {
        metadataBw.close();
      }
    }
  }

  @Override
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

  @Override
  //TODO I should be the one that's providing locking here to make sure that no records are inserted
  // while the columnsToIndex map is updated
  public void index(SortableResult id, Iterable<Index> columnsToIndex) throws TableNotFoundException, UnexpectedStateException, MutationsRejectedException, IOException {
    checkNotNull(id);
    checkNotNull(columnsToIndex);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    final Multimap<Column,Index> columns = mapForIndexedColumns(columnsToIndex);
    final int numCols = columns.keySet().size();
    
    // Add the values of columns to the sortableresult as we want future results to be indexed the same way
    id.addColumnsToIndex(columns.values());
    
    Iterable<MultimapQueryResult> results = fetch(id);
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
    
      // Iterate over the results we have
      for (MultimapQueryResult result : results) {
        
        // If the cardinality of columns is greater in this result than the number of columns
        // we want to index
        if (result.columnSize() > numCols) {
          // It's more efficient to go over each column to index
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
          // Otherwise it's more efficient to iterate over the columns of the result
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

  @Override
  public Iterable<Column> columns(SortableResult id) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    return SortingMetadata.columns(id);
  }

  @Override
  public Iterable<MultimapQueryResult> fetch(SortableResult id) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    BatchScanner bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
    bs.setRanges(Collections.singleton(Range.prefix(id.uuid())));
    bs.fetchColumnFamily(DOCID_FIELD_NAME_TEXT);
    bs.setTimeout(5, TimeUnit.MINUTES);
    
    return Iterables.transform(bs, new KVToMultimap());
  }

  @Override
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(limits);
    
    Iterable<MultimapQueryResult> results = fetch(id);
    
    return new PagedQueryResult<MultimapQueryResult>(results, limits);
  }

  @Override
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Column column, String value) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(column);
    checkNotNull(value);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    BatchScanner bs = null;
    try {
      bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
      bs.setRanges(Collections.singleton(Range.exact(id.uuid() + NULL_BYTE_STR + value)));
      bs.fetchColumnFamily(new Text(column.column()));
      bs.setTimeout(5, TimeUnit.MINUTES);
    
      return Iterables.transform(bs, new KVToMultimap());
    } finally {
      if (null != bs) {
        bs.close();
      }
    }
  }

  @Override
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Column column, String value, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(limits);
    
    Iterable<MultimapQueryResult> results = fetch(id, column, value);
    
    return PagedQueryResult.create(results, limits);
  }

  @Override
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Index ordering) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException {
    return fetch(id, ordering, true);
  }
  
  @Override
  public Iterable<MultimapQueryResult> fetch(SortableResult id, Index ordering, boolean duplicateUidsAllowed) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException {
    checkNotNull(id);
    checkNotNull(ordering);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    Index.define(ordering.column());
    
    if (!id.columnsToIndex().contains(ordering)) {
      log.error("{} is not indexed by {}", ordering, id);
      throw new UnindexedColumnException();
    }
    
    BatchScanner bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
    bs.setRanges(Collections.singleton(Range.prefix(id.uuid())));
    bs.fetchColumnFamily(new Text(ordering.column().column()));
    bs.setTimeout(5, TimeUnit.MINUTES);
    
    // If the client has told us they don't want duplicate records, lets not give them duplicate records
    if (duplicateUidsAllowed){
      return Iterables.transform(bs, new KVToMultimap());
    } else {
      return Iterables.transform(Iterables.filter(bs, new DedupingPredicate()), new KVToMultimap());
    }
  }

  @Override
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Index ordering, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException {
    checkNotNull(id);
    checkNotNull(limits);
    
    Iterable<MultimapQueryResult> results = fetch(id, ordering);
    
    return PagedQueryResult.create(results, limits);
  }

  @Override
  public Iterable<Entry<SValue,Long>> groupResults(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    checkNotNull(column);
    
    Text colf = new Text(column.column());
    
    BatchScanner bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
    bs.setRanges(Collections.singleton(Range.prefix(id.uuid())));
    bs.fetchColumnFamily(colf);
    bs.setTimeout(5, TimeUnit.MINUTES);
    
    IteratorSetting cfg = new IteratorSetting(50, GroupByRowSuffixIterator.class);
    bs.addScanIterator(cfg);
    
    return Iterables.transform(bs, new GroupByFunction());
  }

  @Override
  public PagedQueryResult<Entry<SValue,Long>> groupResults(SortableResult id, Column column, Paging limits) throws TableNotFoundException, UnexpectedStateException, UnindexedColumnException {
    checkNotNull(limits);
    
    Iterable<Entry<SValue,Long>> results = groupResults(id, column);
    
    return PagedQueryResult.create(results, limits);
  }

  @Override
  public void delete(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
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
    
    // TODO be more space efficient here and store a reference to the document once in Accumulo
    // merits: don't bloat the default locality group's index, less size overall
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
