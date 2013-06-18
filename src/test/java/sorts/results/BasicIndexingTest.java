package sorts.results;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sorts.Sorting;
import sorts.SortingMetadata;
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
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    mqr = new MultimapQueryResult(mqr, "2");
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(6, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.finalize(id);
    
    CloseableIterable<MultimapQueryResult> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    
    results.close();
    
    results = s.fetch(id, Index.define("TEXT"));
    
    // This should really be two...
    Assert.assertEquals(4, Iterables.size(results));
    
    results.close();
    s.close();
  }
  
  @Test
  public void totalDeletion() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.singleton(Index.define("TEXT")));
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(3, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.finalize(id);
    
    s.delete(id);
    
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(0, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(0, Iterables.size(scanner));
    
    s.close();
  }
  
  @Test
  public void postIndex() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.<Index> emptySet());
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.index(id, Collections.singleton(Index.define("TEXT")));
    
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(3, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.close();
  }
  
  @Test
  public void addResultsWithIndexOverSparseData() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    Set<Index> columnsToIndex = Sets.newHashSet(Index.define("TEXT"), Index.define("DOESNTEXIST"));
    
    SortableResult id = SortableResult.create(c, AUTHS, columnsToIndex);
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    mqr = new MultimapQueryResult(mqr, "2");
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(6, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.finalize(id);
    
    CloseableIterable<MultimapQueryResult> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    
    results.close();
    s.close();
  }
  
  @Test
  public void postIndexSparseData() throws Exception {
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), SValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), SValue.create("bar", VIZ));
    data.put(Column.create("TEXT"), SValue.create("foobar", VIZ));
    
    MultimapQueryResult mqr = new MultimapQueryResult(data, "1", VIZ);
    
    SortableResult id = SortableResult.create(c, AUTHS, Collections.<Index> emptySet());
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(1, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.index(id, Sets.newHashSet(Index.define("TEXT"), Index.define("DOESNTEXIST")));
    
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(4, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.close();
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
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, mqrs);
    
    s.finalize(id);
    
    PagedQueryResult<MultimapQueryResult> pqr = s.fetch(id, Paging.create(5, 20l));
    
    int pageCount = 0;
    int numRecords = 0;
    for (List<MultimapQueryResult> page : pqr) {
      pageCount++;
      int nextSize = page.size();
      
      Assert.assertTrue(nextSize <= 5);
      
      numRecords += nextSize;
    }
    
    Assert.assertEquals(16, numRecords);
    Assert.assertEquals(4, pageCount);
    
    pqr = s.fetch(id, Paging.create(3, 10l));
    
    pageCount = 0;
    numRecords = 0;
    for (List<MultimapQueryResult> page : pqr) {
      pageCount++;
      int nextSize = page.size();
      
      Assert.assertTrue(nextSize <= 3);
      
      numRecords += nextSize;
    }
    
    Assert.assertEquals(10, numRecords);
    Assert.assertEquals(4, pageCount);
    
    s.close();
  }
  
  @Test
  public void columns() throws Exception {
    SortableResult id = SortableResult.create(c, AUTHS,
        Sets.newHashSet(Index.define("NAME"), Index.define("AGE"), Index.define("HEIGHT"), Index.define("WEIGHT")));
    
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(Column.create("NAME"), SValue.create("George", VIZ));
    data.put(Column.create("AGE"), SValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), SValue.create("70", VIZ));
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "1", VIZ)));
    
    Set<Column> actual = Sets.newHashSet(s.columns(id));
    Set<Column> expected = Sets.newHashSet(data.keySet());
    
    Assert.assertEquals(expected, actual);
    
    data.removeAll(Column.create("HEIGHT"));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "2", VIZ)));
    
    Assert.assertEquals(expected, Sets.newHashSet(s.columns(id)));
    
    data.removeAll(Column.create("AGE"));
    data.put(Column.create("WEIGHT"), SValue.create("100", VIZ));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "3", VIZ)));
    
    expected.add(Column.create("WEIGHT"));
    
    Assert.assertEquals(expected, Sets.newHashSet(s.columns(id)));
    
    s.delete(id);
    
    BatchScanner bs = c.createBatchScanner(id.metadataTable(), id.auths(), 1);
    bs.setRanges(Collections.singleton(Range.exact(id.uuid())));
    bs.fetchColumnFamily(SortingMetadata.COLUMN_COLFAM);
    
    long count = 0;
    for (Entry<Key,Value> e : bs) {
      count++;
    }
    
    bs.close();
    
    Assert.assertEquals(0, count);
    
    s.close();
  }
  
  @Test
  public void projectToSingleValueInColumn() throws Exception {
    Column name = Column.create("NAME"), age = Column.create("AGE"), height = Column.create("HEIGHT"), weight = Column.create("WEIGHT"); 
    
    SortableResult id = SortableResult.create(c, AUTHS, Sets.newHashSet(Index.define(name), Index.define(age),
        Index.define(height), Index.define(weight)));
    
    Multimap<Column,SValue> data = HashMultimap.create();
    
    data.put(name, SValue.create("George", VIZ));
    data.put(Column.create("AGE"), SValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), SValue.create("70", VIZ));
    
    Sorting s = new SortingImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "1", VIZ)));
    
    data.removeAll(name);
    data.put(name, SValue.create("Steve", VIZ));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "2", VIZ)));
    
    data.removeAll(name);
    data.put(name, SValue.create("Frank", VIZ));
    data.put(Column.create("WEIGHT"), SValue.create("100", VIZ));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "3", VIZ)));
    
    CloseableIterable<MultimapQueryResult> iter = s.fetch(id, name, "George");
    ArrayList<MultimapQueryResult> results = Lists.newArrayList(iter);
    iter.close();
    
    Assert.assertEquals(1, results.size());
    
    Assert.assertEquals("1", results.get(0).docId());
    
    data.removeAll(height);
    data.put(height, SValue.create("75", VIZ));
    
    s.addResults(id, Collections.<QueryResult<?>> singleton(new MultimapQueryResult(data, "4", VIZ)));
    
    iter = s.fetch(id, name, "Frank");
    results = Lists.newArrayList(iter);
    iter.close();
    
    Assert.assertEquals(2, results.size());
    
    Set<String> docids = Sets.newHashSet("3", "4");
    
    for (MultimapQueryResult r : results) {
      Assert.assertTrue("Did not find " + r.docId() + " to remove from " + docids, docids.remove(r.docId()));
    }
    
    Assert.assertTrue("Expected empty set of docids: " + docids, docids.isEmpty());
    
    CloseableIterable<MultimapQueryResult> empty = s.fetch(id, age, "0");
    
    Assert.assertEquals(0, Iterables.size(empty));
    
    empty.close();
    s.close();
  }
}
