package sorts.results.integration;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mediawiki.xml.export_0.MediaWikiType;

import sorts.results.impl.MultimapQueryResult;

import com.google.common.collect.Lists;

/**
 * 
 */
@Category(IntegrationTests.class)
public class SortsIntegrationTest extends SortsIntegrationSetup {

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
    List<MultimapQueryResult> results = wikiToMultimap(wiki1);
    
  }
  
}
