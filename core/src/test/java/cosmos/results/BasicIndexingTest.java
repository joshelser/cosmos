/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  Copyright 2013 Josh Elser
 *
 */
package cosmos.results;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import cosmos.Cosmos;
import cosmos.impl.CosmosImpl;
import cosmos.options.Defaults;
import cosmos.options.Index;
import cosmos.options.Order;
import cosmos.options.Paging;
import cosmos.records.Record;
import cosmos.records.RecordValue;
import cosmos.records.impl.MultimapRecord;
import cosmos.store.PersistedStores;
import cosmos.store.Store;
import cosmos.util.IdentitySet;

@RunWith(JUnit4.class)
public class BasicIndexingTest extends AbstractSortableTest {
  
  protected List<Multimap<Column,RecordValue<?>>> data;
  
  @Test
  public void test() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Set<Index> columnsToIndex = Collections.singleton(Index.define("TEXT"));
    
    Store id = Store.create(c, AUTHS, columnsToIndex);
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    mqr = new MultimapRecord(mqr, "2");
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(8, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.finalize(id);
    
    CloseableIterable<MultimapRecord> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    
    results.close();
    
    results = s.fetch(id, Index.define("TEXT"));
    
    // This should really be two, but w/e because we allow dupes
    Assert.assertEquals(4, Iterables.size(results));
    
    results.close();
    
    results = s.fetch(id, Index.define("TEXT"), false);
    
    Assert.assertEquals(2, Iterables.size(results));
    
    results.close();
    
    s.close();
  }
  
  @Test
  public void totalDeletion() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Store id = Store.create(c, AUTHS, Collections.singleton(Index.define("TEXT")));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(4, Iterables.size(scanner));
    
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
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Store id = Store.create(c, AUTHS, Collections.<Index> emptySet());
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.index(id, Collections.singleton(Index.define("TEXT")));
    
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(4, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.close();
  }
  
  @Test
  public void addResultsWithIndexOverSparseData() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Set<Index> columnsToIndex = Sets.newHashSet(Index.define("TEXT"), Index.define("DOESNTEXIST"));
    
    Store id = Store.create(c, AUTHS, columnsToIndex);
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    mqr = new MultimapRecord(mqr, "2");
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(8, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.finalize(id);
    
    CloseableIterable<MultimapRecord> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    
    results.close();
    s.close();
  }
  
  @Test
  public void indexEverything() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("SIZE"), RecordValue.create("2", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Set<Index> columnsToIndex = IdentitySet.<Index> create();
    
    Store id = Store.create(c, AUTHS, columnsToIndex);
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    data = HashMultimap.create();
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    data.put(Column.create("SIZE"), RecordValue.create("3", VIZ));
    
    mqr = new MultimapRecord(data, "2", VIZ);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    // 2 records with 2 columns (forward and reverse), plus the UID pointer (8+2=10)
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(12, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(3, Iterables.size(scanner));
    
    s.finalize(id);
    
    CloseableIterable<MultimapRecord> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    results.close();
    
    // Sort by TEXT: should be docid "2" then "1"
    results = s.fetch(id, Index.define("TEXT"));
    Iterator<MultimapRecord> resultsIter = results.iterator();
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("2", resultsIter.next().docId());
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("1", resultsIter.next().docId());
    
    Assert.assertFalse(resultsIter.hasNext());
    results.close();
    
    // Sort by SIZE: should be docid "1" then "2"
    results = s.fetch(id, Index.define("SIZE"));
    resultsIter = results.iterator();
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("1", resultsIter.next().docId());
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("2", resultsIter.next().docId());
    
    Assert.assertFalse(resultsIter.hasNext());
    results.close();
    
    // Sort by TEXT descending, 1 then 2
    results = s.fetch(id, Index.define("TEXT", Order.DESCENDING));
    
    resultsIter = results.iterator();
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("1", resultsIter.next().docId());
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("2", resultsIter.next().docId());
    
    Assert.assertFalse(resultsIter.hasNext());
    results.close();
    
    // Sort by SIZE descending: should be docid "2" then "1"\
    // TODO Fix this test so I can actually test the reverse over this
    // results = s.fetch(id, Index.define("SIZE", Order.DESCENDING));
    // resultsIter = results.iterator();
    //
    // Assert.assertTrue(resultsIter.hasNext());
    // Assert.assertEquals("2", resultsIter.next().docId());
    //
    // Assert.assertTrue(resultsIter.hasNext());
    // Assert.assertEquals("1", resultsIter.next().docId());
    //
    // Assert.assertFalse(resultsIter.hasNext());
    // results.close();
    
    s.delete(id);
    s.close();
  }
  
  @Test
  public void postIndexSparseData() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("foobar", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Store id = Store.create(c, AUTHS, Collections.<Index> emptySet());
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    Scanner scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.index(id, Sets.newHashSet(Index.define("TEXT"), Index.define("DOESNTEXIST")));
    
    scanner = c.createScanner(Defaults.DATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(5, Iterables.size(scanner));
    
    scanner = c.createScanner(Defaults.METADATA_TABLE, new Authorizations("test"));
    Assert.assertEquals(2, Iterables.size(scanner));
    
    s.close();
  }
  
  @Test
  public void pagedResults() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("foo", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("bar", VIZ));
    data.put(Column.create("TEXT"), RecordValue.create("foobar", VIZ));
    
    List<Record<?>> mqrs = Lists.newArrayListWithCapacity(20);
    for (int i = 0; i < 16; i++) {
      mqrs.add(new MultimapRecord(data, Integer.toString(i), VIZ));
    }
    
    Store id = Store.create(c, AUTHS, Collections.singleton(Index.define("TEXT")));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, mqrs);
    
    s.finalize(id);
    
    PagedResults<MultimapRecord> pqr = s.fetch(id, Paging.create(5, 20));
    
    int pageCount = 0;
    int numRecords = 0;
    for (List<MultimapRecord> page : pqr) {
      pageCount++;
      int nextSize = page.size();
      
      Assert.assertTrue(nextSize <= 5);
      
      numRecords += nextSize;
    }
    
    Assert.assertEquals(16, numRecords);
    Assert.assertEquals(4, pageCount);
    
    pqr = s.fetch(id, Paging.create(3, 10));
    
    pageCount = 0;
    numRecords = 0;
    for (List<MultimapRecord> page : pqr) {
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
    Store id = Store.create(c, AUTHS,
        Sets.newHashSet(Index.define("NAME"), Index.define("AGE"), Index.define("HEIGHT"), Index.define("WEIGHT")));
    
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("NAME"), RecordValue.create("George", VIZ));
    data.put(Column.create("AGE"), RecordValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), RecordValue.create("70", VIZ));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "1", VIZ)));
    
    Set<Column> actual = Sets.newHashSet(s.columns(id));
    Set<Column> expected = Sets.newHashSet(data.keySet());
    
    Assert.assertEquals(expected, actual);
    
    data.removeAll(Column.create("HEIGHT"));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "2", VIZ)));
    
    Assert.assertEquals(expected, Sets.newHashSet(s.columns(id)));
    
    data.removeAll(Column.create("AGE"));
    data.put(Column.create("WEIGHT"), RecordValue.create("100", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "3", VIZ)));
    
    expected.add(Column.create("WEIGHT"));
    
    Assert.assertEquals(expected, Sets.newHashSet(s.columns(id)));
    
    s.delete(id);
    
    BatchScanner bs = c.createBatchScanner(id.metadataTable(), id.auths(), 1);
    bs.setRanges(Collections.singleton(Range.exact(id.uuid())));
    bs.fetchColumnFamily(PersistedStores.COLUMN_COLFAM);
    
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
    
    Store id = Store.create(c, AUTHS, Sets.newHashSet(Index.define(name), Index.define(age), Index.define(height), Index.define(weight)));
    
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(name, RecordValue.create("George", VIZ));
    data.put(Column.create("AGE"), RecordValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), RecordValue.create("70", VIZ));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "1", VIZ)));
    
    data.removeAll(name);
    data.put(name, RecordValue.create("Steve", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "2", VIZ)));
    
    data.removeAll(name);
    data.put(name, RecordValue.create("Frank", VIZ));
    data.put(Column.create("WEIGHT"), RecordValue.create("100", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "3", VIZ)));
    
    CloseableIterable<MultimapRecord> iter = s.fetch(id, name, "George");
    ArrayList<MultimapRecord> results = Lists.newArrayList(iter);
    iter.close();
    
    Assert.assertEquals(1, results.size());
    
    Assert.assertEquals("1", results.get(0).docId());
    
    data.removeAll(height);
    data.put(height, RecordValue.create("75", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "4", VIZ)));
    
    iter = s.fetch(id, name, "Frank");
    results = Lists.newArrayList(iter);
    iter.close();
    
    Assert.assertEquals(2, results.size());
    
    Set<String> docids = Sets.newHashSet("3", "4");
    
    for (MultimapRecord r : results) {
      Assert.assertTrue("Did not find " + r.docId() + " to remove from " + docids, docids.remove(r.docId()));
    }
    
    Assert.assertTrue("Expected empty set of docids: " + docids, docids.isEmpty());
    
    CloseableIterable<MultimapRecord> empty = s.fetch(id, age, "0");
    
    Assert.assertEquals(0, Iterables.size(empty));
    
    empty.close();
    s.close();
  }
  
  @Test
  public void groupResults() throws Exception {
    Column name = Column.create("NAME"), age = Column.create("AGE"), height = Column.create("HEIGHT"), weight = Column.create("WEIGHT");
    
    Store id = Store.create(c, AUTHS, Sets.newHashSet(Index.define(name), Index.define(age), Index.define(height), Index.define(weight)));
    
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(name, RecordValue.create("George", VIZ));
    data.put(Column.create("AGE"), RecordValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), RecordValue.create("70", VIZ));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "1", VIZ)));
    
    data.removeAll(name);
    data.put(name, RecordValue.create("Steve", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "2", VIZ)));
    
    data.removeAll(name);
    data.put(name, RecordValue.create("Frank", VIZ));
    data.put(Column.create("WEIGHT"), RecordValue.create("100", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "3", VIZ)));
    
    // Plain groupBy over three records with unique values in said column
    CloseableIterable<Entry<RecordValue<?>,Long>> groups = s.groupResults(id, name);
    Map<RecordValue<?>,Long> expectedGroups = ImmutableMap.<RecordValue<?>,Long>of(RecordValue.create("George", VIZ), 1l, RecordValue.create("Steve", VIZ), 1l, RecordValue.create("Frank", VIZ), 1l);
    int count = 0;
    for (Entry<RecordValue<?>,Long> group : groups) {
      Assert.assertTrue(expectedGroups.containsKey(group.getKey()));
      Assert.assertEquals(expectedGroups.get(group.getKey()), group.getValue());
      count++;
    }
    
    groups.close();
    
    // Group by weight which only has one value
    groups = s.groupResults(id, weight);
    expectedGroups = ImmutableMap.<RecordValue<?>,Long>of(RecordValue.create("100", VIZ), 1l);
    count = 0;
    for (Entry<RecordValue<?>,Long> group : groups) {
      Assert.assertTrue(expectedGroups.containsKey(group.getKey()));
      Assert.assertEquals(expectedGroups.get(group.getKey()), group.getValue());
      count++;
    }
    
    groups.close();
    
    // Group by height which is the same for everyone
    groups = s.groupResults(id, height);
    expectedGroups = ImmutableMap.<RecordValue<?>,Long>of(RecordValue.create("70", VIZ), 3l);
    count = 0;
    for (Entry<RecordValue<?>,Long> group : groups) {
      Assert.assertTrue(expectedGroups.containsKey(group.getKey()));
      Assert.assertEquals(expectedGroups.get(group.getKey()), group.getValue());
      count++;
    }
    
    groups.close();
    
    Assert.assertEquals(expectedGroups.size(), count);
    
    s.close();
  }
  
  @Test
  public void pagedGroupResults() throws Exception {
    Column name = Column.create("NAME"), age = Column.create("AGE"), height = Column.create("HEIGHT"), weight = Column.create("WEIGHT");
    
    Store id = Store.create(c, AUTHS, Sets.newHashSet(Index.define(name), Index.define(age), Index.define(height), Index.define(weight)));
    
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(name, RecordValue.create("George", VIZ));
    data.put(Column.create("AGE"), RecordValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), RecordValue.create("70", VIZ));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "1", VIZ)));
    
    data.removeAll(name);
    data.put(name, RecordValue.create("Steve", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "2", VIZ)));
    
    data.removeAll(name);
    data.put(name, RecordValue.create("Frank", VIZ));
    data.put(Column.create("WEIGHT"), RecordValue.create("100", VIZ));
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "3", VIZ)));
    
    int pageSize = 1;
    
    // Plain groupBy over three records with unique values in said column
    PagedResults<Entry<RecordValue<?>,Long>> groups = s.groupResults(id, name, Paging.create(pageSize, Integer.MAX_VALUE));
    Map<RecordValue<?>,Long> expectedGroups = ImmutableMap.<RecordValue<?>,Long>of(RecordValue.create("George", VIZ), 1l, RecordValue.create("Steve", VIZ), 1l, RecordValue.create("Frank", VIZ), 1l);
    int count = 0;
    for (List<Entry<RecordValue<?>,Long>> groupPage : groups) {
      Assert.assertEquals(pageSize, groupPage.size());
      
      for (Entry<RecordValue<?>,Long> group : groupPage) {
        Assert.assertTrue(expectedGroups.containsKey(group.getKey()));
        Assert.assertEquals(expectedGroups.get(group.getKey()), group.getValue());
        count++;
      }
    }
    
    groups.close();
    
    Assert.assertEquals(expectedGroups.size(), count);
    
    s.close();
  }
  
  @Test
  public void reverseSorting() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(Column.create("TEXT"), RecordValue.create("aaa", VIZ));
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Set<Index> columnsToIndex = IdentitySet.<Index> create();
    
    Store id = Store.create(c, AUTHS, columnsToIndex);
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    data = HashMultimap.create();
    data.put(Column.create("TEXT"), RecordValue.create("aab", VIZ));
    
    mqr = new MultimapRecord(data, "2", VIZ);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    s.finalize(id);
    
    CloseableIterable<MultimapRecord> results = s.fetch(id);
    
    Assert.assertEquals(2, Iterables.size(results));
    results.close();
    
    // Sort by TEXT descending, 1 then 2
    results = s.fetch(id, Index.define("TEXT", Order.DESCENDING));
    
    Iterator<MultimapRecord> resultsIter = results.iterator();
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("2", resultsIter.next().docId());
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("1", resultsIter.next().docId());
    
    Assert.assertFalse(resultsIter.hasNext());
    results.close();

    // Sort by TEXT descending, 1 then 2
    results = s.fetch(id, Index.define("TEXT", Order.ASCENDING));
    
    resultsIter = results.iterator();
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("1", resultsIter.next().docId());
    
    Assert.assertTrue(resultsIter.hasNext());
    Assert.assertEquals("2", resultsIter.next().docId());
    
    Assert.assertFalse(resultsIter.hasNext());
    results.close();
    
    s.delete(id);
    s.close();
  }

  @Test
  public void addHighColumnCardinalityResults() throws Exception {
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    for (int i = 0; i < 100; i++) { 
      data.put(Column.create("TEXT" + i), RecordValue.create("foo", VIZ));
    }
    
    MultimapRecord mqr = new MultimapRecord(data, "1", VIZ);
    
    Set<Index> columnsToIndex = Sets.newHashSet(Index.define("TEXT0"), Index.define("TEXT1"), Index.define("TEXT2"));
    
    Store id = Store.create(c, AUTHS, Collections.<Index> emptySet());
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(mqr));
    
    // Trigger the case where our record has more columns than what we're indexing
    s.index(id, columnsToIndex);
    
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(1, Iterables.size(s.fetch(id, Index.define("TEXT" + i))));
    }
    
    // Trigger the case where we have more columns than the record
    s.index(id, IdentitySet.<Index> create());
    
    for (int i = 3; i < 100; i++) {
      Assert.assertEquals(1, Iterables.size(s.fetch(id, Index.define("TEXT" + i))));
    }
    
    s.close();
  }
  
  @Test(expected = NoSuchElementException.class)
  public void testNonExistentContent() throws Exception {
    Column name = Column.create("NAME"), age = Column.create("AGE"), height = Column.create("HEIGHT"), weight = Column.create("WEIGHT");
    
    Store id = Store.create(c, AUTHS, Sets.newHashSet(Index.define(name), Index.define(age), Index.define(height), Index.define(weight)));
    
    Multimap<Column,RecordValue<?>> data = HashMultimap.create();
    
    data.put(name, RecordValue.create("George", VIZ));
    data.put(Column.create("AGE"), RecordValue.create("25", VIZ));
    data.put(Column.create("HEIGHT"), RecordValue.create("70", VIZ));
    
    Cosmos s = new CosmosImpl(zkConnectString());
    
    s.register(id);
    
    s.addResults(id, Collections.<Record<?>> singleton(new MultimapRecord(data, "1", VIZ)));
    
    try {
      // A different ID than what we just added
      s.contents(id, "2");
    } finally {
      s.close();
    }
  }
}
