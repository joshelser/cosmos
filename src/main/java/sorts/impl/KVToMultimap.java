package sorts.impl;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.DataInputBuffer;

import sorts.results.impl.MultimapQueryResult;

import com.google.common.base.Function;

/**
 * 
 */
public class KVToMultimap implements Function<Entry<Key,Value>,MultimapQueryResult> {

  public MultimapQueryResult apply(Entry<Key,Value> input) {
    DataInputBuffer buf = new DataInputBuffer();
    buf.reset(input.getValue().get(), input.getValue().getSize());
    
    try {
      return MultimapQueryResult.recreate(buf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
}
