package sorts.accumulo;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
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

import sorts.results.integration.IntegrationTests;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

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
    Connector c = zk.getConnector("root", new PasswordToken("root"));
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      
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
    
    Map<Key,Long> results = ImmutableMap.<Key,Long> builder()
        .put(new Key("1_a", "a", "a", 0), 2l)
        .put(new Key("1_b", "a", "a", 0), 1l)
        .put(new Key("1_c", "a", "a", 0), 3l)
        .build();
    
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
    Connector c = zk.getConnector("root", new PasswordToken("root"));
    
    final String tableName = "foo";

    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      
      for (int i = 1; i < 6; i++) {
        Mutation m = new Mutation(i+ "_a");
        m.put("a", "a", 0, "");
        m.put("a", "b", 0, "");
        
        bw.addMutation(m);
        
        m = new Mutation(i+ "_b");
        m.put("a", "a", 0, "");
        
        bw.addMutation(m);
        
        m = new Mutation(i+ "_c");
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
      Map<Key,Long> results = ImmutableMap.<Key,Long> builder()
          .put(new Key(i + "_a", "a", "a", 0), 2l)
          .put(new Key(i + "_b", "a", "a", 0), 1l)
          .put(new Key(i + "_c", "a", "a", 0), 3l)
          .build();
      
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
    Connector c = zk.getConnector("root", new PasswordToken("root"));
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      
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
    
    Map<Key,Long> results = ImmutableMap.<Key,Long> builder()
        .put(new Key("1_a", "col1", "1", 0), 2l)
        .put(new Key("1_b", "col1", "1", 0), 1l)
        .put(new Key("1_c", "col1", "1", 0), 1l)
        .build();
    
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
    Connector c = zk.getConnector("root", new PasswordToken("root"));
    
    final String tableName = "foo";
    
    if (c.tableOperations().exists(tableName)) {
      c.tableOperations().delete(tableName);
    }
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = null;
    try {
      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      
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
      Map<Key,Long> results = ImmutableMap.<Key,Long> builder()
          .put(new Key(i + "_a", "col1", "1", 0), 2l)
          .put(new Key(i + "_b", "col1", "1", 0), 1l)
          .put(new Key(i + "_c", "col1", "1", 0), 1l)
          .build();
      
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
}
