package sorts.results;

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
    Set<Entry<Column,SValue>> expected = Sets.newHashSet();
    expected.add(Maps.immutableEntry(Column.create("TEXT"),
        SValue.create("foo", new ColumnVisibility("test"))));
    expected.add(Maps.immutableEntry(Column.create("TEXT"),
        SValue.create("bar", new ColumnVisibility("test"))));
    
    Map<String,String> document = Maps.newHashMap();
    document.put("TEXT", "foo");
    document.put("TEXT", "bar");
    
    MapQueryResult mqr = new MapQueryResult(document, "1", new ColumnVisibility("test"),
        new Function<Entry<String,String>,Entry<Column,SValue>>() {

          public Entry<Column,SValue> apply(Entry<String,String> input) {
            final ColumnVisibility cv = new ColumnVisibility("test");
            return Maps.immutableEntry(Column.create(input.getKey()),
                SValue.create(input.getValue(), cv));
          }
    });
    
    for (Entry<Column,SValue> column : mqr.columnValues()) {
      Assert.assertTrue(expected.contains(column));
    }
  }
}
