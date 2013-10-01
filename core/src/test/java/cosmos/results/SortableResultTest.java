package cosmos.results;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

import cosmos.options.Index;
import cosmos.store.Store;

public class SortableResultTest {
  
  private static Connector c;
  
  @BeforeClass
  public static void setupMock() throws Exception {
    MockInstance instance = new MockInstance();
    c = instance.getConnector("user", "password");
  }
  
  // Accumulo 1.5 actually implements locGroups in Mock...
  @Test(expected = NotImplementedException.class)
  public void testLocalityGroupsSetAfterOptimize() throws Exception {
    Store id = Store.create(c, new Authorizations(), Sets.<Index> newHashSet());
    
    Set<Index> indiciesToOptimize = Sets.newHashSet(
        Index.define("COL1"),
        Index.define("COL5"));
    
    id.optimizeIndices(indiciesToOptimize);
    
    Map<String,Set<Text>> locGroups = c.tableOperations().getLocalityGroups(id.dataTable());
    Assert.assertTrue(indiciesToOptimize.size() <= locGroups.size());
    
    for (Set<Text> columns : locGroups.values()) {
      for (Text column : columns) {
        indiciesToOptimize.remove(Index.define(column.toString()));
      }
    }
    
    Assert.assertTrue("Found leftover indicies which had no locality groups defined: " + indiciesToOptimize, indiciesToOptimize.isEmpty());
  }
  
}
