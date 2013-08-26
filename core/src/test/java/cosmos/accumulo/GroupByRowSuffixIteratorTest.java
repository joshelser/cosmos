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
package cosmos.accumulo;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import cosmos.IntegrationTests;
import cosmos.impl.CosmosImpl;


/**
 * 
 */
@Category(IntegrationTests.class)
public class GroupByRowSuffixIteratorTest {
  
  protected static File tmp = Files.createTempDir();
  protected static MiniAccumuloCluster mac;
  protected static MiniAccumuloConfig macConfig;
  
  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    macConfig = new MiniAccumuloConfig(tmp, "root");
    mac = new MiniAccumuloCluster(macConfig);
    
    mac.start();
    
    // Do this now so we don't forget later (or get an exception)
    tmp.deleteOnExit();
  }
  
  @AfterClass
  public static void teardown() throws IOException, InterruptedException {
    mac.stop();
  }
  
  @Test
  public void testSingleRow() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zk.getConnector("root", "root".getBytes());
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, CosmosImpl.DEFAULT_MAX_MEMORY, CosmosImpl.DEFAULT_MAX_LATENCY, CosmosImpl.DEFAULT_MAX_WRITE_THREADS);
      
      Mutation m = new Mutation("1_a");
      m.put("a", "a", 0, "");
      m.put("a", "b", 0, "");
      
      bw.addMutation(m);
      
      m = new Mutation("1_b");
      m.put("a", "a", 0, "");
      
      bw.addMutation(m);
      
      m = new Mutation("1_c");
      m.put("a", "a", 0, "");
      m.put("b", "a", 0, "");
      m.put("c", "a", 0, "");
      
      bw.addMutation(m);
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
    
    Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key("1_a", "a", "b", 0), 2l).put(new Key("1_b", "a", "a", 0), 1l)
        .put(new Key("1_c", "c", "a", 0), 3l).build();
    
    BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
    bs.setRanges(Collections.singleton(new Range()));
    
    try {
      IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
      
      bs.addScanIterator(cfg);
      
      long count = 0;
      for (Entry<Key,Value> entry : bs) {
        VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
        
        Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
        Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
        
        count++;
      }
      
      Assert.assertEquals(results.size(), count);
    } finally {
      if (null != bs) {
        bs.close();
      }
    }
  }
  
  @Test
  public void testMultipleRows() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zk.getConnector("root", "root".getBytes());
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, CosmosImpl.DEFAULT_MAX_MEMORY, CosmosImpl.DEFAULT_MAX_LATENCY, CosmosImpl.DEFAULT_MAX_WRITE_THREADS);
      
      for (int i = 1; i < 6; i++) {
        Mutation m = new Mutation(i + "_a");
        m.put("a", "a", 0, "");
        m.put("a", "b", 0, "");
        
        bw.addMutation(m);
        
        m = new Mutation(i + "_b");
        m.put("a", "a", 0, "");
        
        bw.addMutation(m);
        
        m = new Mutation(i + "_c");
        m.put("a", "a", 0, "");
        m.put("b", "a", 0, "");
        m.put("c", "a", 0, "");
        
        bw.addMutation(m);
      }
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
    
    for (int i = 1; i < 6; i++) {
      Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key(i + "_a", "a", "b", 0), 2l).put(new Key(i + "_b", "a", "a", 0), 1l)
          .put(new Key(i + "_c", "c", "a", 0), 3l).build();
      
      BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
      bs.setRanges(Collections.singleton(Range.prefix(Integer.toString(i))));
      
      try {
        IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
        
        bs.addScanIterator(cfg);
        
        long count = 0;
        for (Entry<Key,Value> entry : bs) {
          VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
          
          Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
          Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
          
          count++;
        }
        
        Assert.assertEquals(results.size(), count);
      } finally {
        if (null != bs) {
          bs.close();
        }
      }
    }
  }
  
  @Test
  public void testSingleRowSingleColumn() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zk.getConnector("root", "root".getBytes());
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, CosmosImpl.DEFAULT_MAX_MEMORY, CosmosImpl.DEFAULT_MAX_LATENCY, CosmosImpl.DEFAULT_MAX_WRITE_THREADS);
      
      Mutation m = new Mutation("1_a");
      m.put("col1", "1", 0, "");
      m.put("col1", "2", 0, "");
      
      bw.addMutation(m);
      
      m = new Mutation("1_b");
      m.put("col1", "1", 0, "");
      
      bw.addMutation(m);
      
      m = new Mutation("1_c");
      m.put("col1", "1", 0, "");
      m.put("col2", "2", 0, "");
      m.put("col3", "1", 0, "");
      
      bw.addMutation(m);
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
    
    Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key("1_a", "col1", "2", 0), 2l).put(new Key("1_b", "col1", "1", 0), 1l)
        .put(new Key("1_c", "col1", "1", 0), 1l).build();
    
    BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
    bs.setRanges(Collections.singleton(new Range()));
    bs.fetchColumnFamily(new Text("col1"));
    
    try {
      IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
      
      bs.addScanIterator(cfg);
      
      long count = 0;
      for (Entry<Key,Value> entry : bs) {
        VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
        
        Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
        Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
        
        count++;
      }
      
      Assert.assertEquals(results.size(), count);
    } finally {
      if (null != bs) {
        bs.close();
      }
    }
  }
  
  @Test
  public void testManyRowsSingleColumn() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zk.getConnector("root", "root".getBytes());
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, CosmosImpl.DEFAULT_MAX_MEMORY, CosmosImpl.DEFAULT_MAX_LATENCY, CosmosImpl.DEFAULT_MAX_WRITE_THREADS);
      
      for (int i = 1; i < 6; i++) {
        Mutation m = new Mutation(i + "_a");
        m.put("col1", "1", 0, "");
        m.put("col1", "2", 0, "");
        
        bw.addMutation(m);
        
        m = new Mutation(i + "_b");
        m.put("col1", "1", 0, "");
        
        bw.addMutation(m);
        
        m = new Mutation(i + "_c");
        m.put("col1", "1", 0, "");
        m.put("col2", "2", 0, "");
        m.put("col3", "1", 0, "");
        
        bw.addMutation(m);
      }
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
    
    for (int i = 1; i < 6; i++) {
      Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key(i + "_a", "col1", "2", 0), 2l).put(new Key(i + "_b", "col1", "1", 0), 1l)
          .put(new Key(i + "_c", "col1", "1", 0), 1l).build();
      
      BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
      bs.setRanges(Collections.singleton(Range.prefix(Integer.toString(i))));
      bs.fetchColumnFamily(new Text("col1"));
      
      try {
        IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
        
        bs.addScanIterator(cfg);
        
        long count = 0;
        for (Entry<Key,Value> entry : bs) {
          VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
          
          Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
          Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
          
          count++;
        }
        
        Assert.assertEquals(results.size(), count);
      } finally {
        if (null != bs) {
          bs.close();
        }
      }
    }
  }
  
  @Test
  public void testReseek() throws Exception {
    TreeMap<Key,Value> data = Maps.newTreeMap();
    data.put(new Key("foo\u0000bell", "RESTAURANT", "f\u00001"), new Value());
    data.put(new Key("foo\u0000bell", "RESTAURANT", "f\u00002"), new Value());
    data.put(new Key("foo\u0000taco", "RESTAURANT", "f\u00001"), new Value());
    data.put(new Key("foo\u0000taco", "RESTAURANT", "f\u00002"), new Value());
    data.put(new Key("foo\u0000taco", "RESTAURANT", "f\u00003"), new Value());
    
    SortedMapIterator source = new SortedMapIterator(data);
    
    GroupByRowSuffixIterator iter = new GroupByRowSuffixIterator();
    iter.init(source, Collections.<String,String> emptyMap(), new IteratorEnvironment() {
      @Override
      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(final String mapFileName) throws IOException {
        return null;
      }
      
      @Override
      public AccumuloConfiguration getConfig() {
        return null;
      }
      
      @Override
      public IteratorScope getIteratorScope() {
        return null;
      }
      
      @Override
      public boolean isFullMajorCompaction() {
        return false;
      }
      
      @Override
      public void registerSideChannel(final SortedKeyValueIterator<Key,Value> iter) {}
    });
    
    iter.seek(Range.prefix("foo"), Collections.<ByteSequence> emptySet(), false);
    
    Assert.assertTrue(iter.hasTop());
    
    Assert.assertEquals(new Key("foo\u0000bell", "RESTAURANT", "f\u00002"), iter.getTopKey());
    VLongWritable actual = new VLongWritable(), expected = new VLongWritable(2);
    
    actual.readFields(new DataInputStream(new ByteArrayInputStream(iter.getTopValue().get())));
    Assert.assertEquals(expected, actual);
    
    iter.seek(new Range(new Key("foo\u0000bell", "RESTAURANT", "f\u00002"), false, new Key("fop"), false), Collections.<ByteSequence> emptySet(), false);
    
    Assert.assertTrue(iter.hasTop());
    
    Assert.assertEquals(new Key("foo\u0000taco", "RESTAURANT", "f\u00003"), iter.getTopKey());
    actual = new VLongWritable();
    expected = new VLongWritable(3);
    
    actual.readFields(new DataInputStream(new ByteArrayInputStream(iter.getTopValue().get())));
    Assert.assertEquals(expected, actual);
    
    iter.next();
    
    Assert.assertFalse(iter.hasTop());
  }
}
