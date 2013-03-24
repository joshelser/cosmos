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

public class SortingMetadata {
  public static final Text EMPTY_TEXT = new Text("");
  public static final Text STATE_COLFAM = new Text("state");
  
  public enum State {
    LOADING,
    LOADED,
    ERROR,
    DELETING,
    UNKNOWN
  }
  
  public State getState(SortableResult id) throws TableNotFoundException {
    checkNotNull(id);
    
    Connector con = id.connector();
    
    Scanner s = con.createScanner(id.metadataTable, Constants.NO_AUTHS);
    
    s.setRange(new Range(id.uuid()));
    
    s.fetchColumnFamily(STATE_COLFAM);
    
    Iterator<Entry<Key,Value>> iter = s.iterator();
    
    if (iter.hasNext()) {
      Entry<Key,Value> stateEntry = iter.next();
      return deserializeState(stateEntry.getValue());
    }
    
    return State.UNKNOWN;
  }
  
  public void setState(SortableResult id, State state) throws TableNotFoundException, MutationsRejectedException {
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
  
  public static State deserializeState(Value v) {
    return State.valueOf(v.toString());
  }
}
