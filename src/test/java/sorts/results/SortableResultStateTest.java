package sorts.results;

import java.util.Collections;

import org.apache.accumulo.core.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import sorts.Sorting;
import sorts.SortingMetadata;
import sorts.SortingMetadata.State;
import sorts.impl.SortableResult;
import sorts.impl.SortingImpl;
import sorts.options.Index;

/**
 * 
 */
@RunWith(JUnit4.class)
public class SortableResultStateTest extends AbstractSortableTest {
  
  @Test
  public void test() throws Exception {
    SortableResult id = SortableResult.create(c, Constants.NO_AUTHS, Collections.<Index> emptySet());
    
    Assert.assertEquals(State.UNKNOWN, SortingMetadata.getState(id));
        
    Sorting s = new SortingImpl();
    s.register(id);
    
    Assert.assertEquals(State.LOADING, SortingMetadata.getState(id));
    
    s.finalize(id);
    
    Assert.assertEquals(State.LOADED, SortingMetadata.getState(id));
    
    // Would be State.DELETING during this call
    s.delete(id);
    
    Assert.assertEquals(State.UNKNOWN, SortingMetadata.getState(id));
  }
  
}
