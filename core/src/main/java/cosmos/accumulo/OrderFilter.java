package cosmos.accumulo;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import cosmos.options.Defaults;

import com.google.common.base.Preconditions;

/**
 * 
 */
public class OrderFilter extends Filter {
  
  public static final String PREFIX = "prefix";
  
  protected Text _holder;
  
  protected String cqPrefix = null;
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source,  options, env);
    this._holder = new Text();
    validateOptions(options);
  }

  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (!super.validateOptions(options)) {
      throw new IllegalArgumentException("Could not initialize OrderFilter");
    }
    
    if (!options.containsKey(PREFIX)) {
      throw new IllegalArgumentException("No prefix was provided: " + PREFIX);
    }
    
    this.cqPrefix = options.get(PREFIX);
    
    return true;
  }
  
  @Override
  public boolean accept(Key k, Value v) {
    Preconditions.checkNotNull(_holder);
    
    k.getColumnQualifier(_holder);
    
    int index = _holder.find(Defaults.NULL_BYTE_STR);
    
    // Found a null
    if (-1 != index) {
      try {
        String prefix = Text.decode(_holder.getBytes(), 0, index);
        return this.cqPrefix.equals(prefix);
      } catch (CharacterCodingException e) {
        throw new RuntimeException(e);
      }
    }
    
    return false;
  }
  
}
