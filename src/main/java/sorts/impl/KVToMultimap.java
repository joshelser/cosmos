package sorts.impl;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 */
public class KVToMultimap implements Function<Entry<Key,Value>,QueryResult<?>> {

  public MultimapQueryResult apply(Entry<Key,Value> input) {
    Multimap<Column,sorts.results.Value> dontCare = HashMultimap.create();
    
    dontCare.put(Column.create("ATALL"),sorts.results.Value.create(input.getValue().toString(), input.getKey().getColumnVisibilityParsed()));
    
    Text cq = input.getKey().getColumnQualifier();
    
    int index = cq.find(SortingImpl.NULL_BYTE_STR);
    
    if (-1 == index) {
      throw new IllegalStateException("Could not find null separator in qualified for " + input.getKey());
    }
    
    String docId;
    try {
      docId = Text.decode(cq.getBytes(), index + 1, (cq.getLength() - (index + 1)));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(e);
    }
    
    return new MultimapQueryResult(dontCare, docId, input.getKey().getColumnVisibilityParsed());
  }
  
}
