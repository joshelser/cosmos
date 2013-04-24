package sorts.results;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sorts.Sorting;
import sorts.impl.SortableResult;
import sorts.impl.SortingImpl;
import sorts.options.Defaults;
import sorts.options.Index;
import sorts.options.Paging;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@RunWith(JUnit4.class)
public class BasicIndexingTest extends AbstractSortableTest {
  
  protected List<Multimap<Column,SValue>> data;
  
  @Test
  public void test() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    Set<Index> columnsToIndex = Collections.singleton(Index.define("TEXT"));
    
    SortableResult id = SortableResult.create(c, AUTHS, columnsToIndex);
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    mqr = new MultimapQueryResult(mqr, "2");
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(6, Iterables.size(scanner));

    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    s.finalize(id);
    
    Iterable<MultimapQueryResult> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    
    results = s.fetch(id, Index.define("TEXT"));
    
    // This should really be two...
    Assert.assertEquals(4, Iterables.size(results));
  }
  
  @Test
  public void totalDeletion() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.singleton(Index.define("TEXT")));
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));

    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(3, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    s.finalize(id);
    
    s.delete(id);

    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(0, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(0, Iterables.size(scanner));
  }
  
  @Test
  public void postIndex() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.<Index> emptySet());
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));

    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    s.index(id, Collections.singleton(Index.define("TEXT")));

    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(3, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
  }
  
  @Test
  public void addResultsWithIndexOverSpareData() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    Set<Index> columnsToIndex = Sets.newHashSet(Index.define("TEXT"), Index.define("DOESNTEXIST"));
    
    SortableResult id = SortableResult.create(c, AUTHS, columnsToIndex);
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    mqr = new MultimapQueryResult(mqr, "2");
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));

    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(6, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    s.finalize(id);
    
    Iterable<MultimapQueryResult> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
  }
  
  @Test
  public void postIndexSparseData() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    data.put(Column.create("TEXT"), SValue.create("foobar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.<Index> emptySet());
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));

    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    s.index(id, Sets.newHashSet(Index.define("TEXT"), Index.define("DOESNTEXIST")));
    
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(4, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
  }
  
  @Test
  public void pagedResults() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    data.put(Column.create("TEXT"), SValue.create("foobar", VIZ));
    
    List<QueryResult<?>> mqrs = Lists.newArrayListWithCapacity(20);
    for (int i = 0; i < 16; i++) {
      mqrs.add(new MultimapQueryResult(data, Integer.toString(i), VIZ));
    }
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.singleton(Index.define("TEXT")));
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    
    s.addResults(id, mqrs);
    
    s.finalize(id);
    
    PagedQueryResult pqr = s.fetch(id, Paging.create(5, 20l));
    
    int pageCount = 0;
    int numRecords = 0;
    while (pqr.hasNext()) {
      pageCount++;
      int nextSize = pqr.next().size();
      
      Assert.assertTrue(nextSize <= 5);
      
      numRecords += nextSize;
    }
    
    Assert.assertEquals(16, numRecords);
    Assert.assertEquals(4, pageCount);
    
    pqr = s.fetch(id, Paging.create(3, 10l));
    
    pageCount = 0;
    numRecords = 0;
    while (pqr.hasNext()) {
      pageCount++;
      int nextSize = pqr.next().size();
      
      Assert.assertTrue(nextSize <= 3);
      
      numRecords += nextSize;
    }
    
    Assert.assertEquals(10, numRecords);
    Assert.assertEquals(4, pageCount);
  }
  
}
