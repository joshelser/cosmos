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
import org.apache.accumulo.test.MiniAccumuloCluster;
import org.apache.accumulo.test.MiniAccumuloConfig;
import org.apache.hadoop.io.VLongWritable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

/**
 * 
 */
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
  public void test1() throws Exception {
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zk.getConnector("root", new PasswordToken("root"));
    
    final String tableName = "foo";
    
    c.tableOperations().create(tableName);
    
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    
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
    bw.close();
    
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
}
