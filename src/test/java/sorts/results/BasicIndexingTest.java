package sorts.results;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
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
import com.google.common.collect.Multimap;

@RunWith(JUnit4.class)
public class BasicIndexingTest extends AbstractSortableTest {
  
  protected List<Multimap<Column,SValue>> data;
  
  @Test
  public void test() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS);
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    Set<Index> columnsToIndex = Collections.singleton(Index.define("TEXT"));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr), columnsToIndex);
    
    mqr = new MultimapQueryResult(mqr, "2");
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr), columnsToIndex);
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    for (Entry<Key,Value> entry : scanner) {
      System.out.println(entry);
    }
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    for (Entry<Key,Value> entry : scanner) {
      System.out.println(entry);
    }
    
    s.finalize(id);
    
    Iterable<MultimapQueryResult> results = s.fetch(id);
    
    for (MultimapQueryResult result : results) {
      System.out.println(result.docId() + " " + result.document());
    }
  }
  
  @Test
  public void totalDeletion() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS);
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr), Collections.singleton(Index.define("TEXT")));
    
    int metadataCount = 0, dataCount = 0;
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    for (Entry<Key,Value> entry : scanner) {
      dataCount++;
    }
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    for (Entry<Key,Value> entry : scanner) {
      metadataCount++;
    }
    
    Assert.assertNotEquals(0, metadataCount);
    Assert.assertNotEquals(0, dataCount);
    
    s.finalize(id);
    
    s.delete(id);
    
    metadataCount = 0;
    dataCount = 0;
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    for (Entry<Key,Value> entry : scanner) {
      dataCount++;
    }
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    for (Entry<Key,Value> entry : scanner) {
      metadataCount++;
    }
    
    Assert.assertEquals(0, metadataCount);
    Assert.assertEquals(0, dataCount);
  }
  
}
