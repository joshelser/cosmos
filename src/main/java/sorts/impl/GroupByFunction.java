package sorts.impl;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;

import sorts.accumulo.GroupByRowSuffixIterator;
import sorts.options.Defaults;
import sorts.results.SValue;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * 
 */
public class GroupByFunction implements Function<Entry<Key,Value>,Entry<SValue,Long>> {

  private final Text _holder = new Text();
  
  @Override
  public Entry<SValue,Long> apply(Entry<Key,Value> entry) {
    String value = getValueFromKey(entry.getKey());
    
    //TODO Add Cache for CV
    SValue sval = SValue.create(value, entry.getKey().getColumnVisibilityParsed());
    VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
    
    return Maps.immutableEntry(sval, writable.get());
  }
  
  private String getValueFromKey(Key k) {
    Preconditions.checkNotNull(k);
    
    k.getRow(_holder);
    
    int index = _holder.find(Defaults.NULL_BYTE_STR);
    
    if (-1 == index) {
      throw new IllegalArgumentException("Found no null byte in key: " + k);
    }
    
    try {
      return Text.decode(_holder.getBytes(), index + 1, _holder.getLength() - (index + 1));
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(e);
    }
  }
  
}
