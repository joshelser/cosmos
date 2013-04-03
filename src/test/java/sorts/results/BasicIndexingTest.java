package sorts.results;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sorts.SortableResult;
import sorts.Sorting;
import sorts.impl.SortingImpl;
import sorts.options.Defaults;
import sorts.options.Index;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

@RunWith(JUnit4.class)
public class BasicIndexingTest extends AbstractSortableTest {
  
  @Test
  public void test() throws Exception {
    Multimap<Column,Value> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), Value.create("foo", new ColumnVisibility("test")));
    data.put(Column.create("TEXT"), Value.create("bar", new ColumnVisibility("test")));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", new ColumnVisibility("test"));
    
    SortableResult id = SortableResult.create(c, new Authorizations("test"));
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    Set<Entry<Column,Index>> columnsToIndex = Collections.singleton(Maps.immutableEntry(Column.create("TEXT"), Index.define("TEXT")));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr), columnsToIndex);
    
    mqr = new MultimapQueryResult(mqr, "2");
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr), columnsToIndex);
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    for (Entry<Key,org.apache.accumulo.core.data.Value> entry : scanner) {
      System.out.println(entry);
    }
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    for (Entry<Key,org.apache.accumulo.core.data.Value> entry : scanner) {
      System.out.println(entry);
    }
    
    s.finalize(id);
    
    Iterable<QueryResult<?>> results = s.fetch(id);
    
    for (QueryResult<?> result : results) {
      System.out.println(result.docId() + " " + result.document());
    }
  }
  
}
