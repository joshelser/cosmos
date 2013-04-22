package sorts.results.integration;

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.MiniAccumuloCluster;
import org.apache.accumulo.test.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mediawiki.xml.export_0.MediaWikiType;

import sorts.Sorting;
import sorts.impl.SortableResult;
import sorts.impl.SortingImpl;
import sorts.options.Defaults;
import sorts.options.Index;
import sorts.options.Ordering;
import sorts.results.Column;
import sorts.results.QueryResult;
import sorts.results.SValue;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * 
 */
@Category(IntegrationTests.class)
public class SortsIntegrationTest extends SortsIntegrationSetup {
  protected static MiniAccumuloCluster mac;
  
  @BeforeClass
  public static void createAccumuloCluster() throws Exception {
    File macDir = File.createTempFile("miniaccumulocluster", null);
    Assert.assertTrue(macDir.delete());
    Assert.assertTrue(macDir.mkdir());
    macDir.deleteOnExit();
    
    MiniAccumuloConfig config = new MiniAccumuloConfig(macDir, "");
    config.setNumTservers(4);
    
    mac = new MiniAccumuloCluster(config);
    mac.start();
    
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector c = zk.getConnector("root", new PasswordToken(""));
    
    // Add in auths for "en"
    c.securityOperations().changeUserAuthorizations("root", new Authorizations("en"));
  }
  
  @AfterClass
  public static void stopAccumuloCluster() throws Exception {
    mac.stop();
  }
  
  @Test
  public void test() throws Exception {
    // Cache all of the wikis -- multithreaded
    loadAllWikis();
    
    long start = System.currentTimeMillis();
    
    // These should all be cached
    Assert.assertNotNull(getWiki1());
    Assert.assertNotNull(getWiki2());
    Assert.assertNotNull(getWiki3());
    Assert.assertNotNull(getWiki4());
    Assert.assertNotNull(getWiki5());
    
    long end = System.currentTimeMillis();
    
    Assert.assertTrue((end - start) < 10000);
  }
  
  @Test
  public void testWiki1() throws Exception {
    // Get the same wiki 3 times
    List<Thread> threads = Lists.newArrayList();
    
    for (int i = 0; i < 3; i++) {
      threads.add(new Thread(new Runnable() {
        public void run() {
          try {
            getWiki1();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }));
    }
    
    long start = System.currentTimeMillis();
    
    for (Thread t : threads) {
      t.start();
    }
    
    for (Thread t : threads) {
      t.join();
    }
    
    long end = System.currentTimeMillis();
    
    // We should only have to wait on one to parse the xml
    Assert.assertTrue((end - start) < 8000);
  }
  
  @Test
  public void wiki1Test() throws Exception {
    MediaWikiType wiki1 = getWiki1();
    List<QueryResult<?>> results = wikiToMultimap(wiki1);
    
    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector con = zk.getConnector("root", new PasswordToken(""));
    con.tableOperations().create(Defaults.DATA_TABLE);
    con.tableOperations().create(Defaults.METADATA_TABLE);
    
    SortableResult id = SortableResult.create(con, new Authorizations("en"),
        Sets.newHashSet(Index.define(PAGE_ID)));
    
    Sorting s = new SortingImpl();
    
    s.register(id);
    s.addResults(id, results);
    
    Column pageId = Column.create(PAGE_ID);
    
    Iterable<MultimapQueryResult> newResults = s.fetch(id);
    //Iterable<MultimapQueryResult> newResults = s.fetch(id, Ordering.create(pageId));
    
    Assert.assertNotNull(newResults);
    
    long count = 0;
    for (MultimapQueryResult res : newResults) {
      Collection<SValue> pageIds = res.get(pageId);
      System.out.println(pageIds + ":" + res);
      count++;
    }
    
    Assert.assertEquals(wiki1.getPage().size(), count);
  }
  
}
