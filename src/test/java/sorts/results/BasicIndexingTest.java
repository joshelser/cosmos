package sorts.results;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Before;
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
public class BasicIndexingTest {
  
  private Connector c;
  
  @Before
  public void setup() throws Exception {
    MockInstance mi = new MockInstance();
    Properties p = new Properties();
    p.setProperty("password", "");
    c = mi.getConnector("root", p);
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("test"));
    c.tableOperations().create(Defaults.DATA_TABLE);
    c.tableOperations().create(Defaults.METADATA_TABLE);
  }
  
  @Test
  public void test() throws Exception {
    Multimap<Column,Value> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), Value.create("foo", new ColumnVisibility("test")));
    data.put(Column.create("TEXT"), Value.create("bar", new ColumnVisibility("test")));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", new ColumnVisibility("test"));
    
    SortableResult id = SortableResult.create(c);
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr), Collections.singleton(
    		Maps.immutableEntry(Column.create("TEXT"), Index.define("TEXT"))));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    for (Entry<Key,org.apache.accumulo.core.data.Value> entry : scanner) {
      System.out.println(entry);
    }
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    for (Entry<Key,org.apache.accumulo.core.data.Value> entry : scanner) {
      System.out.println(entry);
    }
  }
  
}
