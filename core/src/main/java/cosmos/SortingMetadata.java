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
package cosmos;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

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
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;

import cosmos.impl.Store;
import cosmos.results.CloseableIterable;
import cosmos.results.Column;

public class SortingMetadata {
  public static final Text EMPTY_TEXT = new Text("");
  public static final Text STATE_COLFAM = new Text("state");
  public static final Text COLUMN_COLFAM = new Text("column");
  
  /**
   * A {@link State} determines the lifecycle phases of a {@link Store} in Accumulo.
   * 
   * <p>
   * {@code LOADING} means that new records are actively being loaded and queries can start; however, only the columns specified as being indexed when the
   * {@link Store} was defined can be guaranteed to exist. Meaning, calls to {@link Cosmos#index(Store, Iterable)} will not block queries from
   * running while the index is being updated. Obviously, queries in this state are not guaranteed to be the column result set for a {@link Store}
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
}
