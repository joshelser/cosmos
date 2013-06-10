package sorts.results.integration;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import sorts.Sorting;
import sorts.impl.SortableResult;
import sorts.impl.SortingImpl;
import sorts.options.Defaults;
import sorts.options.Index;
import sorts.results.CloseableIterable;
import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.SValue;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * 
 */
@Category(IntegrationTests.class)
public class GroupByIntegrationTest extends SortsIntegrationSetup {
  
  public static MiniAccumuloConfig conf;
  public static MiniAccumuloCluster mac;
  
  protected Sorting sorts;
  protected ZooKeeperInstance inst;
  protected Connector con;
  
  protected static TestingServer zk;
  
  @BeforeClass
  public static void setupMAC() throws Exception {
    File tmp = Files.createTempDir();
    tmp.deleteOnExit();
    conf = new MiniAccumuloConfig(tmp, "foo");
    mac = new MiniAccumuloCluster(conf);
    mac.start();

    
    ZooKeeperInstance zkInst = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector con = zkInst.getConnector("root", new PasswordToken("foo"));
    con.tableOperations().create(Defaults.DATA_TABLE);
    con.tableOperations().create(Defaults.METADATA_TABLE);

    zk = new TestingServer();
  }
  
  @AfterClass
  public static void stopMAC() throws Exception {
    mac.stop();
  }
  
  @Before
  public void setupSorts() throws Exception {
    sorts = new SortingImpl(zk.getConnectString());
    inst = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    con = inst.getConnector("root", new PasswordToken("foo"));
  }
  
  @Test
  public void simpleNonSparseRecordTest() throws Exception {
    SortableResult id = SortableResult.create(con, new Authorizations(), Sets.newHashSet(Index.define("NAME"),
        Index.define("AGE"), Index.define("STATE"), Index.define("COLOR")));
    
    // Register the ID with the implementation
    sorts.register(id);
    
    final ColumnVisibility cv = new ColumnVisibility("");
    
    Multimap<Column,SValue> data = HashMultimap.create();
    data.put(Column.create("NAME"), SValue.create("Josh", cv));
    data.put(Column.create("AGE"), SValue.create("24", cv));
    data.put(Column.create("STATE"), SValue.create("MD", cv));
    data.put(Column.create("COLOR"), SValue.create("Blue", cv));
    
    MultimapQueryResult mqr1 = new MultimapQueryResult(data, "1", cv);
    
    data = HashMultimap.create();
    data.put(Column.create("NAME"), SValue.create("Wilhelm", cv));
    data.put(Column.create("AGE"), SValue.create("25", cv));
    data.put(Column.create("STATE"), SValue.create("MD", cv));
    data.put(Column.create("COLOR"), SValue.create("Pink", cv));
    
    MultimapQueryResult mqr2 = new MultimapQueryResult(data, "2", cv);
    
    data = HashMultimap.create();
    data.put(Column.create("NAME"), SValue.create("Marky", cv));
    data.put(Column.create("AGE"), SValue.create("29", cv));
    data.put(Column.create("STATE"), SValue.create("MD", cv));
    data.put(Column.create("COLOR"), SValue.create("Blue", cv));
    
    MultimapQueryResult mqr3 = new MultimapQueryResult(data, "3", cv);
    
    // Add our data
    sorts.addResults(id, Lists.<QueryResult<?>> newArrayList(mqr3, mqr2, mqr1));
    
    // Testing NAME
    Map<SValue,Long> expects = ImmutableMap.<SValue,Long> of(
        SValue.create("Josh", cv), 1l, 
        SValue.create("Wilhelm", cv), 1l, 
        SValue.create("Marky", cv), 1l);
    
    CloseableIterable<Entry<SValue,Long>> countedResults = sorts.groupResults(id, Column.create("NAME"));
    
    int resultCount = 0;
    for (Entry<SValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }
    
    countedResults.close();
    Assert.assertEquals(expects.size(), resultCount);
  
    // TEesting AGE
    expects = ImmutableMap.<SValue,Long> of(
        SValue.create("24", cv), 1l, 
        SValue.create("25", cv), 1l, 
        SValue.create("29", cv), 1l);
    
    countedResults = sorts.groupResults(id, Column.create("AGE"));
    
    resultCount = 0;
    for (Entry<SValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }
    
    countedResults.close();
    
    Assert.assertEquals(expects.size(), resultCount);
  
    // Testing STATE
    expects = ImmutableMap.<SValue,Long> of(SValue.create("MD", cv), 3l);
    
    countedResults = sorts.groupResults(id, Column.create("STATE"));
    
    resultCount = 0;
    for (Entry<SValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }

    countedResults.close();
    
    Assert.assertEquals(expects.size(), resultCount);
  
    // Testing COLOR
    expects = ImmutableMap.<SValue,Long> of(
        SValue.create("Blue", cv), 2l, 
        SValue.create("Pink", cv), 1l);
    
    countedResults = sorts.groupResults(id, Column.create("COLOR"));
    
    resultCount = 0;
    for (Entry<SValue,Long> entry : countedResults) {
      resultCount++;
      
      Assert.assertTrue(expects.containsKey(entry.getKey()));
      Assert.assertEquals(expects.get(entry.getKey()), entry.getValue());
    }
    
    countedResults.close();
    
    Assert.assertEquals(expects.size(), resultCount);
  }
  
}
