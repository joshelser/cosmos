package sorts.results;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sorts.results.impl.MapQueryResult;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@RunWith(JUnit4.class)
public class MapQueryResultTest {
  
  @Test
  public void basicCreation() {
    Set<Entry<Column,Value>> expected = Sets.newHashSet();
    expected.add(Maps.immutableEntry(Column.create(ByteBuffer.wrap("TEXT".getBytes())),
        Value.create(ByteBuffer.wrap("foo".getBytes()), new ColumnVisibility("test"))));
    expected.add(Maps.immutableEntry(Column.create(ByteBuffer.wrap("TEXT".getBytes())),
        Value.create(ByteBuffer.wrap("bar".getBytes()), new ColumnVisibility("test"))));
    
    Map<String,String> document = Maps.newHashMap();
    document.put("TEXT", "foo");
    document.put("TEXT", "bar");
    
    MapQueryResult mqr = new MapQueryResult(document, ByteBuffer.wrap("1".getBytes()), new ColumnVisibility("test"),
        new Function<Entry<String,String>,Entry<Column,Value>>() {

          public Entry<Column,Value> apply(Entry<String,String> input) {
            final ColumnVisibility cv = new ColumnVisibility("test");
            return Maps.immutableEntry(Column.create(ByteBuffer.wrap(input.getKey().getBytes())),
                Value.create(ByteBuffer.wrap(input.getValue().getBytes()), cv));
          }
    });
    
    for (Entry<Column,Value> column : mqr.columnValues()) {
      Assert.assertTrue(expected.contains(column));
    }
  }
}
