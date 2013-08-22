/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;

import cosmos.Cosmos;
import cosmos.SortingMetadata;
import cosmos.SortingMetadata.State;
import cosmos.UnexpectedStateException;
import cosmos.UnindexedColumnException;
import cosmos.accumulo.GroupByRowSuffixIterator;
import cosmos.accumulo.OrderFilter;
import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.options.Order;
import cosmos.options.Paging;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.results.PagedQueryResult;
import cosmos.results.QueryResult;
import cosmos.results.SValue;
import cosmos.results.impl.MultimapQueryResult;
import cosmos.util.IndexHelper;
import cosmos.util.Single;

public class CosmosImpl implements Cosmos {
  private static final Logger log = LoggerFactory.getLogger(CosmosImpl.class);
  
  public static final long LOCK_SECS = 10;
  
  private final BatchWriterConfig DEFAULT_BW_CONFIG = new BatchWriterConfig();
  private final CuratorFramework curator;
  private final ReverseLexicoder<String> revLex = new ReverseLexicoder<String>(new StringLexicoder());
  
  public CosmosImpl(String zookeepers) {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(2000, 3);
    curator = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy);
    curator.start();
    
    // TODO http://curator.incubator.apache.org/curator-recipes/shared-reentrant-lock.html
    // "Error handling: ... strongly recommended that you add a ConnectionStateListener and
    // watch for SUSPENDED and LOST state changes"
    
    // curator.getConnectionStateListenable().addListener(new ConnectionStateListener() {
    //
    // @Override
    // public void stateChanged(CuratorFramework client, ConnectionState newState) {
    //
    // }
    //
    // });
  }
  
  @Override
  public void close() {
    synchronized (curator) {
      CuratorFrameworkState state = curator.getState();
      
      // Stop unless we're already stopped
      if (!CuratorFrameworkState.STOPPED.equals(state)) {
        try {
          Closeables.close(curator, true);
        } catch (IOException e) {
          log.warn("Caught IOException closing Curator connection", e);
        }
      }
    }
  }
  
  /**
   * finalize is not guaranteed to be called, as such, care should be taken to ensure that {@link close} is called.
   */
  @Override
  public void finalize() throws IOException {
    this.close();
  }
  
  @Override
  public void register(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.UNKNOWN.equals(s)) {
        UnexpectedStateException e = unexpectedState(id, State.UNKNOWN, s);
        log.error(e.getMessage());
        throw e;
      }
      
      State targetState = State.LOADING;
      
      log.debug("Setting state for {} from {} to {}", new Object[] {id, s, targetState});
      
      SortingMetadata.setState(id, targetState);
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:register", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }
  
  @Override
  public void addResult(SortableResult id, QueryResult<?> queryResult) throws Exception {
    checkNotNull(queryResult);
    
    Stopwatch sw = new Stopwatch().start();
    try {
      addResults(id, Single.<QueryResult<?>> create(queryResult));
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:addResult", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }
  
  @Override
  public void addResults(SortableResult id, Iterable<? extends QueryResult<?>> queryResults) throws Exception {
    checkNotNull(id);
    checkNotNull(queryResults);
    
    Stopwatch sw = new Stopwatch().start();
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s)) {
        // stopwatch closed in finally
        UnexpectedStateException e = unexpectedState(id, State.LOADING, s);
        log.error(e.getMessage());
        throw e;
      }
      
      InterProcessMutex lock = getMutex(id);
      
      // TODO We don't need to lock on multiple calls to addResults; however, we need to lock over adding the
      // new records to make sure a call to index() doesn't come in while we're processing a stale set of Columns to index
      boolean locked = false;
      int count = 1;
      
      if (id.lockOnUpdates()) {
        while (!locked && count < 4) {
          if (locked = lock.acquire(10, TimeUnit.SECONDS)) {
            try {
              performAdd(id, queryResults);
            } finally {
              // Don't hog the lock
              lock.release();
            }
          } else {
            count++;
            log.warn("addResults() on {} could not acquire lock after {} seconds. Attempting acquire #{}", new Object[] {id.uuid(), LOCK_SECS, count});
          }
          
          throw new IllegalStateException("Could not acquire lock during index() after " + count + " attempts");
        }
      } else {
        performAdd(id, queryResults);
      }
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:addResults", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }
  
  protected void performAdd(SortableResult id, Iterable<? extends QueryResult<?>> queryResults) throws MutationsRejectedException, TableNotFoundException,
      IOException {
    BatchWriter bw = null, metadataBw = null;
    
    try {
      // Add the values of columns to the sortableresult as we want
      Set<Index> columnsToIndex = id.columnsToIndex();
      
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
      metadataBw = id.connector().createBatchWriter(id.metadataTable(), DEFAULT_BW_CONFIG);
      
      final IndexHelper indexHelper = IndexHelper.create(columnsToIndex);
      final Text holder = new Text();
      final Set<Column> columnsAlreadyIndexed = Sets.newHashSet();
      
      for (QueryResult<?> result : queryResults) {
        bw.addMutation(addDocument(id, result));
        Mutation columnMutation = new Mutation(id.uuid());
        boolean newColumnIndexed = false;
        
        for (Entry<Column,SValue> entry : result.columnValues()) {
          final Column c = entry.getKey();
          final SValue v = entry.getValue();
          
          if (!columnsAlreadyIndexed.contains(c)) {
            holder.set(c.column());
            columnMutation.put(SortingMetadata.COLUMN_COLFAM, holder, Defaults.EMPTY_VALUE);
            columnsAlreadyIndexed.add(c);
            newColumnIndexed = true;
          }
          
          if (indexHelper.shouldIndex(c)) {
            for (Index index : indexHelper.indicesForColumn(c)) {
              Mutation m = getDocumentPrefix(id, result, v.value(), index.order());
              
              final String direction = Order.direction(index.order());
              m.put(index.column().toString(), direction + Defaults.NULL_BYTE_STR + result.docId(), v.visibility(), Defaults.EMPTY_VALUE);
              
              bw.addMutation(m);
            }
          }
        }
        
        if (newColumnIndexed) {
          metadataBw.addMutation(columnMutation);
        }
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
    } catch (IOException e) {
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
    
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s)) {
        throw unexpectedState(id, State.LOADING, s);
      }
      
      final State desiredState = State.LOADED;
      
      log.debug("Changing state for {} from {} to {}", new Object[] {id, s, desiredState});
      
      SortingMetadata.setState(id, desiredState);
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:finalize", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }
  
  @Override
  public void index(SortableResult id, Set<Index> columnsToIndex) throws Exception {
    checkNotNull(id);
    checkNotNull(columnsToIndex);
    
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
        // stopwatch stopped by finally
        throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
      }
      
      InterProcessMutex lock = getMutex(id);
      
      boolean locked = false;
      int count = 1;
      
      // Only perform locking when the client requests it
      if (id.lockOnUpdates) {
        while (!locked && count < 4) {
          if (locked = lock.acquire(10, TimeUnit.SECONDS)) {
            try {
              performUpdate(id, columnsToIndex);
              
            } finally {
              lock.release();
            }
            
            return;
          } else {
            count++;
            log.warn("index() on {} could not acquire lock after {} seconds. Attempting acquire #{}", new Object[] {id.uuid(), LOCK_SECS, count});
          }
          
          throw new IllegalStateException("Could not acquire lock during index() after " + count + " attempts");
        }
      } else {
        performUpdate(id, columnsToIndex);
      }
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:index", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }
  
  protected void performUpdate(SortableResult id, Set<Index> columnsToIndex) throws TableNotFoundException, UnexpectedStateException,
      MutationsRejectedException, IOException {
    final IndexHelper indexHelper = IndexHelper.create(columnsToIndex);
    final int numCols = indexHelper.columnCount();
    CloseableIterable<MultimapQueryResult> results = null;
    BatchWriter bw = null;
    
    try {
      // Add the values of columns to the sortableresult as we want future results to be indexed the same way
      id.addColumnsToIndex(columnsToIndex);
      
      // Get the results we have to update
      results = fetch(id);
      
      bw = id.connector().createBatchWriter(id.dataTable(), DEFAULT_BW_CONFIG);
      
      // Iterate over the results we have
      for (MultimapQueryResult result : results) {
        
        // If the cardinality of columns is greater in this result than the number of columns
        // we want to index
        if (result.columnSize() > numCols) {
          // It's more efficient to go over each column to index
          for (Column columnToIndex : indexHelper.columnIndices().keySet()) {
            
            // Determine if the object contains the column we need to index
            if (result.containsKey(columnToIndex)) {
              // If so, get the value(s) for that column
              final Collection<Index> indices = indexHelper.indicesForColumn(columnToIndex);
              final Collection<SValue> values = result.get(columnToIndex);
              
              addIndicesForRecord(id, result, bw, indices, values);
            }
          }
        } else {
          // Otherwise it's more efficient to iterate over the columns of the result
          for (Entry<Column,SValue> entry : result.columnValues()) {
            final Column column = entry.getKey();
            
            // Determine if we should index this column
            if (indexHelper.shouldIndex(column)) {
              final Collection<Index> indices = indexHelper.indicesForColumn(column);
              final Collection<SValue> values = result.get(column);
              
              addIndicesForRecord(id, result, bw, indices, values);
            }
          }
        }
      }
    } finally {
      if (null != bw) {
        bw.close();
      }
      if (null != results) {
        results.close();
      }
    }
  }
  
  /**
   * For a QueryResult, write the Index(es) for the Column the SValues came from.
   * 
   * @param id
   * @param result
   * @param bw
   * @param indices
   * @param values
   * @throws MutationsRejectedException
   * @throws IOException
   */
  protected void addIndicesForRecord(SortableResult id, MultimapQueryResult result, BatchWriter bw, Collection<Index> indices, Collection<SValue> values)
      throws MutationsRejectedException, IOException {
    // Place an Index entry for each value in each direction defined
    for (Index index : indices) {
      for (SValue value : values) {
        Mutation m = getDocumentPrefix(id, result, value.value(), index.order());
        
        final String direction = Order.direction(index.order());
        m.put(index.column().toString(), direction + Defaults.NULL_BYTE_STR + result.docId(), value.visibility(), Defaults.EMPTY_VALUE);
        bw.addMutation(m);
      }
    }
  }
  
  @Override
  public Iterable<Column> columns(SortableResult id) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
        // Stopwatch stopped by finally
        throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
      }
      
      return SortingMetadata.columns(id);
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:columns", sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }
  
  @Override
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    
    final String description = "Cosmos:fetch";
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
        sw.stop();
        id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
        throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
      }
      
      BatchScanner bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
      bs.setRanges(Collections.singleton(Range.prefix(id.uuid())));
      bs.fetchColumnFamily(Defaults.DOCID_FIELD_NAME_TEXT);
      
      // Handles stoping the stopwatch
      return CloseableIterable.transform(bs, new IndexToMultimapQueryResult(this, id), id.tracer(), description, sw);
    } catch (TableNotFoundException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    } catch (UnexpectedStateException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    } catch (RuntimeException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    }
    // no finally as the trace is stopped by the CloseableIterable
  }
  
  @Override
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Paging limits) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(limits);
    
    CloseableIterable<MultimapQueryResult> results = fetch(id);
    
    return new PagedQueryResult<MultimapQueryResult>(results, limits);
  }
  
  @Override
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id, Column column, String value) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(column);
    checkNotNull(value);
    
    final String description = "Cosmos:fetchWithColumnValue";
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
        sw.stop();
        id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
        
        throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
      }
      
      BatchScanner bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
      bs.setRanges(Collections.singleton(Range.exact(id.uuid() + Defaults.NULL_BYTE_STR + value)));
      bs.fetchColumnFamily(new Text(column.column()));
      
      return CloseableIterable.transform(bs, new IndexToMultimapQueryResult(this, id), id.tracer(), description, sw);
    } catch (TableNotFoundException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    } catch (UnexpectedStateException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    } catch (RuntimeException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    }
    // no finally as the trace is stopped by the CloseableIterable
  }
  
  @Override
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Column column, String value, Paging limits) throws TableNotFoundException,
      UnexpectedStateException {
    checkNotNull(limits);
    
    CloseableIterable<MultimapQueryResult> results = fetch(id, column, value);
    
    return PagedQueryResult.create(results, limits);
  }
  
  @Override
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id, Index ordering) throws TableNotFoundException, UnexpectedStateException,
      UnindexedColumnException {
    return fetch(id, ordering, true);
  }
  
  @Override
  public CloseableIterable<MultimapQueryResult> fetch(SortableResult id, Index ordering, boolean duplicateUidsAllowed) throws TableNotFoundException,
      UnexpectedStateException, UnindexedColumnException {
    checkNotNull(id);
    checkNotNull(ordering);
    
    final String description = "Cosmos:fetchWithIndex";
    Stopwatch sw = new Stopwatch().start();
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
        sw.stop();
        throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
      }
      
      Index.define(ordering.column());
      
      if (!id.columnsToIndex().contains(ordering)) {
        log.error("{} is not indexed by {}", ordering, id);
        
        sw.stop();
        id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
        
        throw new UnindexedColumnException();
      }
      
      Scanner scanner = id.connector().createScanner(id.dataTable(), id.auths());
      scanner.setRange(Range.prefix(id.uuid()));
      scanner.fetchColumnFamily(new Text(ordering.column().column()));
      scanner.setBatchSize(200);
      
      // Filter on cq-prefix to only look at the ordering we want
      IteratorSetting filter = new IteratorSetting(50, "cqFilter", OrderFilter.class);
      filter.addOption(OrderFilter.PREFIX, Order.direction(ordering.order()));
      scanner.addScanIterator(filter);
      
      // If the client has told us they don't want duplicate records, lets not give them duplicate records
      if (duplicateUidsAllowed) {
        return CloseableIterable.transform(scanner, new IndexToMultimapQueryResult(this, id), id.tracer(), description, sw);
      } else {
        return CloseableIterable.filterAndTransform(scanner, new DedupingPredicate(), new IndexToMultimapQueryResult(this, id), id.tracer(), description, sw);
      }
    } catch (TableNotFoundException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    } catch (UnexpectedStateException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    } catch (RuntimeException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS));
      throw e;
    }
    // no finally as the trace is stopped by the CloseableIterable
  }
  
  @Override
  public PagedQueryResult<MultimapQueryResult> fetch(SortableResult id, Index ordering, Paging limits) throws TableNotFoundException, UnexpectedStateException,
      UnindexedColumnException {
    checkNotNull(id);
    checkNotNull(limits);
    
    CloseableIterable<MultimapQueryResult> results = fetch(id, ordering);
    
    return PagedQueryResult.create(results, limits);
  }
  
  @Override
  public CloseableIterable<Entry<SValue,Long>> groupResults(SortableResult id, Column column) throws TableNotFoundException, UnexpectedStateException,
      UnindexedColumnException {
    checkNotNull(id);
    
    Stopwatch sw = new Stopwatch().start();
    final String description = "Cosmos:groupResults";
    
    try {
      State s = SortingMetadata.getState(id);
      
      if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
        sw.stop();
        throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
      }
      
      checkNotNull(column);
      
      Text colf = new Text(column.column());
      
      BatchScanner bs = id.connector().createBatchScanner(id.dataTable(), id.auths(), 10);
      bs.setRanges(Collections.singleton(Range.prefix(id.uuid())));
      bs.fetchColumnFamily(colf);
      
      // Filter on cq-prefix to only look at the ordering we want
      IteratorSetting filter = new IteratorSetting(50, "cqFilter", OrderFilter.class);
      filter.addOption(OrderFilter.PREFIX, Order.FORWARD);
      bs.addScanIterator(filter);
      
      IteratorSetting cfg = new IteratorSetting(60, GroupByRowSuffixIterator.class);
      bs.addScanIterator(cfg);
      
      return CloseableIterable.transform(bs, new GroupByFunction(), id.tracer(), description, sw);
    } catch (TableNotFoundException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description,  sw.elapsed(TimeUnit.MILLISECONDS)); 
      throw e;
    } catch (UnexpectedStateException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS)); 
      throw e;
    } catch (RuntimeException e) {
      // In the exceptional case, stop the timer
      sw.stop();
      id.tracer().addTiming(description, sw.elapsed(TimeUnit.MILLISECONDS)); 
      throw e;
    }
    // no finally as the trace is stopped by the CloseableIterable
  }
  
  @Override
  public PagedQueryResult<Entry<SValue,Long>> groupResults(SortableResult id, Column column, Paging limits) throws TableNotFoundException,
      UnexpectedStateException, UnindexedColumnException {
    checkNotNull(limits);
    
    CloseableIterable<Entry<SValue,Long>> results = groupResults(id, column);
    
    return PagedQueryResult.create(results, limits);
  }
  
  @Override
  public MultimapQueryResult contents(SortableResult id, String docId) throws TableNotFoundException, UnexpectedStateException {
    checkNotNull(id);
    checkNotNull(docId);
    
    // Omit tracing here just due to sheer magnitude of these calls.
    
    State s = SortingMetadata.getState(id);
    
    if (!State.LOADING.equals(s) && !State.LOADED.equals(s)) {
      throw unexpectedState(id, new State[] {State.LOADING, State.LOADED}, s);
    }
    
    Scanner scanner = id.connector().createScanner(id.dataTable(), id.auths());
    scanner.setRange(Range.exact(id.uuid() + Defaults.NULL_BYTE_STR + docId));
    scanner.fetchColumnFamily(Defaults.CONTENTS_COLFAM_TEXT);
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    if (!iter.hasNext()) {
      scanner.close();
      
      throw new NoSuchElementException("No such result for " + docId + " in " + id.uuid());
    } else {
      Value value = iter.next().getValue();
      scanner.close();
      
      return KeyValueToMultimapQueryResult.transform(value);
    }
  }
  
  @Override
  public void delete(SortableResult id) throws TableNotFoundException, MutationsRejectedException, UnexpectedStateException {
    checkNotNull(id);
    
    Stopwatch sw = new Stopwatch().start();
    
    try {
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
    } finally {
      sw.stop();
      id.tracer().addTiming("Cosmos:delete", sw.elapsed(TimeUnit.MILLISECONDS));
      
      // Be nice and when the client deletes these results, automatically flush the traces for them too
      id.sendTraces();
    }
  }
  
  protected Mutation getDocumentPrefix(SortableResult id, QueryResult<?> queryResult, String suffix, Order order) {
    final Text t = new Text();
    byte[] b = id.uuid().getBytes();
    t.append(b, 0, b.length);
    t.append(new byte[] {0}, 0, 1);
    if (Order.ASCENDING.equals(order)) {
      t.append(suffix.getBytes(), 0, suffix.getBytes().length);
    } else {
      b = this.revLex.encode(suffix);
      t.append(b, 0, b.length);
    }
    
    return new Mutation(t);
  }
  
  protected Mutation addDocument(SortableResult id, QueryResult<?> queryResult) throws IOException {
    Mutation m = getDocumentPrefix(id, queryResult, queryResult.docId(), Order.ASCENDING);
    
    // Store the docId as a searchable entry
    m.put(Defaults.DOCID_FIELD_NAME, Order.FORWARD + Defaults.NULL_BYTE_STR + queryResult.docId(), queryResult.documentVisibility(), Defaults.EMPTY_VALUE);
    
    // Write the contents for this record once
    m.put(Defaults.CONTENTS_COLFAM_TEXT, new Text(), queryResult.documentVisibility(), queryResult.toValue());
    
    return m;
  }
  
  protected UnexpectedStateException unexpectedState(SortableResult id, State[] expected, State actual) {
    return new UnexpectedStateException("Invalid state " + id + " for " + id + ". Expected one of " + Arrays.asList(expected) + " but was " + actual);
  }
  
  protected UnexpectedStateException unexpectedState(SortableResult id, State expected, State actual) {
    return new UnexpectedStateException("Invalid state " + id + " for " + id + ". Expected " + expected + " but was " + actual);
  }
  
  protected final InterProcessMutex getMutex(SortableResult id) {
    return new InterProcessMutex(curator, Defaults.CURATOR_PREFIX + id.uuid());
  }
  
}
