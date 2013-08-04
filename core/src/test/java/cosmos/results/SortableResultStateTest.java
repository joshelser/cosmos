package cosmos.results;

import java.util.Collections;

import org.apache.accumulo.core.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import cosmos.Cosmos;
import cosmos.SortingMetadata;
import cosmos.SortingMetadata.State;
import cosmos.impl.CosmosImpl;
import cosmos.impl.SortableResult;
import cosmos.options.Index;

/**
 * 
 */
@RunWith(JUnit4.class)
public class SortableResultStateTest extends AbstractSortableTest {
  
  @Test
  public void test() throws Exception {
    SortableResult id = SortableResult.create(c, Constants.NO_AUTHS, Collections.<Index> emptySet());
    
    Assert.assertEquals(State.UNKNOWN, SortingMetadata.getState(id));
        
    Cosmos s = new CosmosImpl(zk.getConnectString());
    s.register(id);
    
    Assert.assertEquals(State.LOADING, SortingMetadata.getState(id));
    
    s.finalize(id);
    
    Assert.assertEquals(State.LOADED, SortingMetadata.getState(id));
    
    // Would be State.DELETING during this call
    s.delete(id);
    
    Assert.assertEquals(State.UNKNOWN, SortingMetadata.getState(id));
    
    s.close();
  }
  
}
