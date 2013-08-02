package sorts.impl;

import java.nio.charset.CharacterCodingException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import sorts.Sorting;
import sorts.UnexpectedStateException;
import sorts.options.Defaults;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.base.Function;

/**
 * 
 */
public class IndexToMultimapQueryResult implements Function<Entry<Key,Value>,MultimapQueryResult> {
  
  protected final Sorting sorts;
  protected final SortableResult id;
  
  public IndexToMultimapQueryResult(Sorting sorts, SortableResult id) {
    this.sorts = sorts;
    this.id = id;
  }
  
  @Override
  public MultimapQueryResult apply(Entry<Key,Value> input) {
    Key k = input.getKey();
    
    Text colqual = k.getColumnQualifier();
    
    int index = colqual.find(Defaults.NULL_BYTE_STR);
    if (-1 == index) {
      throw new RuntimeException("Was provided unexpected Key: " + k);
    }
    
    int start = index + 1;
    try {
      String docId = Text.decode(colqual.getBytes(), start, colqual.getLength() - start);
      
      return sorts.contents(id, docId);
      
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (UnexpectedStateException e) {
      throw new RuntimeException(e);
    } catch (CharacterCodingException e) {
      throw new RuntimeException(e);
    }
  }
  
}
