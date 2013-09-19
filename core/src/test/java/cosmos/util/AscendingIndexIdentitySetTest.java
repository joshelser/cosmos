package cosmos.util;

import org.junit.Assert;
import org.junit.Test;

import cosmos.options.Index;
import cosmos.options.Order;

public class AscendingIndexIdentitySetTest {
  
  @Test
  public void test() {
    AscendingIndexIdentitySet set = AscendingIndexIdentitySet.create();
    
    // By default, indexes are 'forward' or 'ASCENDING'
    Assert.assertTrue(set.contains(Index.define("FOO")));
    Assert.assertTrue(set.contains(Index.define("FOO", Order.ASCENDING)));
    Assert.assertFalse(set.contains(Index.define("FOO", Order.DESCENDING)));
  }
  
}
