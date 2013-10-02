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
package cosmos.store;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import cosmos.Cosmos;
import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.options.Order;
import cosmos.protobuf.StoreProtobuf;
import cosmos.protobuf.StoreProtobuf.IndexSpec;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;
import cosmos.util.AscendingIndexIdentitySet;
import cosmos.util.DescendingIndexIdentitySet;
import cosmos.util.IdentitySet;

public class PersistedStores {
  private static final Logger log = LoggerFactory.getLogger(PersistedStores.class);
  private static final LoadingCache<String,Class<?>> CLASS_CACHE = CacheBuilder.newBuilder().build(new CacheLoader<String,Class<?>>() {
    
    @Override
    public Class<?> load(String typeClassName) throws ExecutionException {
      try {
        return Class.forName(typeClassName);
      } catch (ClassNotFoundException e) {
        throw new ExecutionException("Could not load type class for " + typeClassName, e);
      }
    }
    
  });
  
  public static final Text EMPTY_TEXT = new Text("");
  public static final Text STATE_COLFAM = new Text("state");
  public static final Text COLUMN_COLFAM = new Text("column");
  public static final Text SERIALIZED_STORE_COLFAM = new Text("store");
  
  /**
   * A {@link State} determines the lifecycle phases of a {@link Store} in Accumulo.
   * 
   * <p>
   * {@code LOADING} means that new records are actively being loaded and queries can start; however, only the columns specified as being indexed when the
   * {@link Store} was defined can be guaranteed to exist. Meaning, calls to {@link Cosmos#index(Store, Iterable)} will not block queries from running while the
   * index is being updated. Obviously, queries in this state are not guaranteed to be the column result set for a {@link Store}
   * 
   * <p>
   * {@code LOADED} means that the {@link Cosmos} client writing results has completed.
   * 
   * <p>
   * {@code ERROR} means that there an error in the loading of the data for the given {@link Store} and processing has ceased.
   * 
   * <p>
   * {@code DELETING} means that a client has called {@link Cosmos#delete(Store)} and the results are in the process of being deleted.
   * 
   * <p>
   * {@code UNKNOWN} means that the software is unaware of the given {@link Store}
   * 
   * 
   */
  public enum State {
    LOADING, LOADED, ERROR, DELETING, UNKNOWN
  }
  
  public static Collection<Store> listStores(Connector connector, String metadataTable, Authorizations auths) throws InvalidProtocolBufferException, TableNotFoundException
  {	    Scanner s = connector.createScanner(metadataTable,auths);
	    
	    s.setRange(new Range());
	    
	    s.fetchColumnFamily(SERIALIZED_STORE_COLFAM);
	    
	    Iterator<Entry<Key,Value>> iter = s.iterator();
	    
	    List<Store> stores = Lists.newArrayList();
	    if (iter.hasNext()) {
	      Entry<Key,Value> stateEntry = iter.next();
	      stores.add(  deserialize(connector,stateEntry.getValue()) );
	    }
	    
	    s.close();
	    
	  return stores;
  }
  
  public static State getState(Store id) throws TableNotFoundException {
    checkNotNull(id);
    
    Connector con = id.connector();
    
    Scanner s = con.createScanner(id.metadataTable(), Constants.NO_AUTHS);
    
    s.setRange(new Range(id.uuid()));
    
    s.fetchColumnFamily(STATE_COLFAM);
    
    Iterator<Entry<Key,Value>> iter = s.iterator();
    
    if (iter.hasNext()) {
      Entry<Key,Value> stateEntry = iter.next();
      return deserializeState(stateEntry.getValue());
    }
    
    return State.UNKNOWN;
  }
  
  public static void setState(Store id, State state) throws TableNotFoundException, MutationsRejectedException {
    checkNotNull(id);
    checkNotNull(state);
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.metadataTable(), new BatchWriterConfig());
      Mutation m = new Mutation(id.uuid());
      m.put(STATE_COLFAM, EMPTY_TEXT, new Value(state.toString().getBytes()));
      
      bw.addMutation(m);
      bw.flush();
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
  }
  
  public static void remove(Store id) throws TableNotFoundException, MutationsRejectedException {
    checkNotNull(id);
    
    BatchDeleter bd = null;
    try {
      bd = id.connector().createBatchDeleter(id.metadataTable(), id.auths(), 10, new BatchWriterConfig());
      bd.setRanges(Collections.singleton(Range.exact(id.uuid())));
      bd.delete();
    } finally {
      if (null != bd) {
        bd.close();
      }
    }
  }
  
  public static State deserializeState(Value v) {
    return State.valueOf(v.toString());
  }
  
  /**
   * Return the {@link Column}s that exist for the given {@link Store}
   * 
   * @param id
   * @return
   * @throws TableNotFoundException
   */
  public static CloseableIterable<Column> columns(Store id, String description, Stopwatch sw) throws TableNotFoundException {
    checkNotNull(id);
    
    BatchScanner bs = id.connector().createBatchScanner(id.metadataTable(), id.auths(), 10);
    bs.setRanges(Collections.singleton(Range.exact(id.uuid())));
    bs.fetchColumnFamily(COLUMN_COLFAM);
    
    return CloseableIterable.transform(bs, new Function<Entry<Key,Value>,Column>() {
      private final Text holder = new Text();
      
      @Override
      public Column apply(Entry<Key,Value> input) {
        input.getKey().getColumnQualifier(holder);
        return Column.create(holder.toString());
      }
      
    }, id.tracer(), description, sw);
  }
  
  /**
   * Serialize this store to the metadata table as defined in the {@link Store}
   * 
   * @param id
   * @param connector
   * @throws TableNotFoundException
   * @throws MutationsRejectedException
   */
  public static void store(Store id) throws TableNotFoundException, MutationsRejectedException {
    checkNotNull(id);
    
    Mutation m = new Mutation(id.uuid());
    m.put(SERIALIZED_STORE_COLFAM, Defaults.EMPTY_TEXT, serialize(id));
    
    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.metadataTable(), new BatchWriterConfig());
      bw.addMutation(m);
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
  }
  
  /**
   * Given a {@link Connector}, some {@link Authorizations}, a uuid and a table name, try to reconstitute
   * a {@link Store} serialized in said table.
   * @param connector
   * @param metadataTable
   * @param auths
   * @param uuid
   * @return
   * @throws TableNotFoundException
   * @throws InvalidProtocolBufferException
   */
  public static Store retrieve(Connector connector, String metadataTable, Authorizations auths, String uuid) throws TableNotFoundException {
    checkNotNull(connector);
    checkNotNull(uuid);
    
    Scanner s = connector.createScanner(metadataTable, auths);
    s.fetchColumnFamily(SERIALIZED_STORE_COLFAM);
    s.setRange(Range.exact(uuid));
    
    Iterator<Entry<Key,Value>> iter = s.iterator();
    if (!iter.hasNext()) {
      throw new NoSuchElementException(uuid);
    }
    
    try {
      return deserialize(connector, iter.next().getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Could not deserialize protocol buffer for Store " + uuid, e);
    }
  }
  
  public static Value serialize(Store id) {
    checkNotNull(id);
    
    StoreProtobuf.Store.Builder storeBuilder = StoreProtobuf.Store.newBuilder();
    
    storeBuilder.setDataTable(id.dataTable());
    storeBuilder.setMetadataTable(id.metadataTable());
    storeBuilder.setUniqueId(id.uuid());
    storeBuilder.setAuths(id.auths().serialize());
    storeBuilder.setLockOnUpdates(id.lockOnUpdates());
    
    Set<Index> indexes = id.columnsToIndex();
    
    // Check for the Ascending/Descending identity sets first
    if (indexes instanceof AscendingIndexIdentitySet) {
      storeBuilder.setIndexSpec(IndexSpec.ASCENDING_IDENTITY);
    } else if (indexes instanceof DescendingIndexIdentitySet) {
      storeBuilder.setIndexSpec(IndexSpec.DESCENDING_IDENTITY);
    } else if (IdentitySet.class.isAssignableFrom(indexes.getClass())) {
      // Check if it's still an identity set.
      // If so, log that if it's something more specific than what we know about
      // (we may lose some information encapsulated in the implementation)
      if (!(indexes instanceof IdentitySet)) {
        log.warn("Found unrecognized IdentitySet implementation {} in {}, treating as IdentitySet", indexes.getClass(), id);
      }
      
      storeBuilder.setIndexSpec(IndexSpec.IDENTITY);
    } else {
      // If it's not one of the IdentitySets, treat it as a regular Set
      storeBuilder.setIndexSpec(IndexSpec.OTHER);
      
      for (Index i : id.columnsToIndex()) {
        StoreProtobuf.Index protobufIndex = i.toProtobufIndex();
        storeBuilder.addIndexes(protobufIndex);
      }
    }
    
    return new Value(storeBuilder.build().toByteArray());
  }
  
  public static Store deserialize(Connector connector, Value value) throws InvalidProtocolBufferException {
    checkNotNull(connector);
    checkNotNull(value);
    
    StoreProtobuf.Store store = StoreProtobuf.Store.parseFrom(value.get());
    
    Set<Index> columnsToIndex;
    
    // Using the IndexSpec, determine what information to read from the message
    // to appropriate create the columnsToIndex Set
    switch (store.getIndexSpec()) {
      case ASCENDING_IDENTITY: {
        columnsToIndex = AscendingIndexIdentitySet.create();
        break;
      }
      case DESCENDING_IDENTITY: {
        columnsToIndex = DescendingIndexIdentitySet.create();
        break;
      }
      case IDENTITY: {
        columnsToIndex = IdentitySet.<Index> create();
        break;
      }
      case OTHER: {
        // If we don't have an IdentitySet of some kind, assume it's a "regular"
        // concretely-backed Set
        List<StoreProtobuf.Index> serializedIndexes = store.getIndexesList();
        columnsToIndex = Sets.newHashSetWithExpectedSize(serializedIndexes.size());
        for (StoreProtobuf.Index i : serializedIndexes) {
          Column column = Column.create(i.getColumn());
          String typeClassName = i.getType();
          
          Order order;
          switch (i.getOrder()) {
            case ASCENDING: {
              order = Order.ASCENDING;
              break;
            }
            case DESCENDING: {
              order = Order.DESCENDING;
              break;
            }
            default: {
              throw new RuntimeException("Found unknown order: " + i.getOrder());
            }
          }
          
          // Load the class, hitting a cache when we can
          Class<?> typeClass;
          try {
            typeClass = CLASS_CACHE.get(typeClassName);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
          
          columnsToIndex.add(Index.define(column, order, typeClass));
        }
        
        break;
      }
      default: {
        throw new RuntimeException("Unable to process unknown Index specification: " + store.getIndexSpec());
      }
    }
    
    Authorizations auths = new Authorizations(store.getAuths().getBytes());
    
    return Store.create(connector, auths, store.getUniqueId(), columnsToIndex, store.getLockOnUpdates(), store.getDataTable(), store.getMetadataTable());
  }
}
