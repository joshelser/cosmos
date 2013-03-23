package sorts.results;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sorts.results.impl.MapQueryResult;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

@RunWith(JUnit4.class)
public class MapQueryResultTest {
  
  @Test
  public void basicCreation() {
    Map<String,String> document = Maps.newHashMap();
    document.put("TEXT", "foo");
    document.put("TEXT", "bar");
    
    MapQueryResult mqr = new MapQueryResult(document, ByteBuffer.wrap("1".getBytes()), 
        new Function<Entry<String,String>,Entry<Column,Value>>() {

          public Entry<Column,Value> apply(Entry<String,String> input) {
            final ColumnVisibility cv = new ColumnVisibility("test");
            return Maps.immutableEntry(Column.create(ByteBuffer.wrap(input.getKey().getBytes())),
                Value.create(ByteBuffer.wrap(input.getValue().getBytes()), cv));
          }
    });
  }
}
