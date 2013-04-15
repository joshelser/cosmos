package sorts;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
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

import sorts.impl.SortableResult;

public class SortingMetadata {
  public static final Text EMPTY_TEXT = new Text("");
  public static final Text STATE_COLFAM = new Text("state");
  
  /**
   * A {@link State} determines the lifecycle phases of a {@link SortableResult}
   * in Accumulo.
   * 
   * <p>
   * {@code LOADING} means that new records are actively being loaded and queries 
   * can start; however, only the columns specified as being indexed when the {@link SortableResult}
   * was defined can be guaranteed to exist. Meaning, calls to {@link Sorting#index(SortableResult, Iterable)}
   * will not block queries from running while the index is being updated. Obviously, queries in this
   * state are not guaranteed to be the column result set for a {@link SortableResult}
   * 
   * <p>
   *  {@code LOADED} means that the {@link Sorting} client writing results has completed. 
   * 
   * <p>
   * {@code ERROR} means that there an error in the loading of the data for the given
   * {@link SortableResult} and processing has ceased.
   * 
   * <p>
   * {@code DELETING} means that a client has called {@link Sorting#delete(SortableResult)}
   * and the results are in the process of being deleted.
   * 
   * <p>
   * {@code UNKNOWN} means that the software is unaware of the given {@link SortableResult}
   *  
   * 
   */
  public enum State {
    LOADING,
    LOADED,
    ERROR,
    DELETING,
    UNKNOWN
  }
  
  public static State getState(SortableResult id) throws TableNotFoundException {
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
  
  public static void setState(SortableResult id, State state) throws TableNotFoundException, MutationsRejectedException {
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
  
  public static void remove(SortableResult id) throws TableNotFoundException, MutationsRejectedException {
    checkNotNull(id);

    BatchWriter bw = null;
    try {
      bw = id.connector().createBatchWriter(id.metadataTable(), new BatchWriterConfig());
      Mutation m = new Mutation(id.uuid());
      m.putDelete(STATE_COLFAM, EMPTY_TEXT);
      
      bw.addMutation(m);
      bw.flush();
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
  }
  
  public static State deserializeState(Value v) {
    return State.valueOf(v.toString());
  }
}
