package cosmos.util;

import cosmos.options.Index;
import cosmos.options.Order;

public class DescendingIndexIdentitySet extends IdentitySet<Index> {
  
  private static final DescendingIndexIdentitySet INSTANCE = new DescendingIndexIdentitySet();
  
  @SuppressWarnings("unchecked")
  public static DescendingIndexIdentitySet create() {
    return INSTANCE;
  }
  
  @Override
  public boolean contains(Object o) {
    if (o instanceof Index) {
      return Order.DESCENDING.equals(((Index) o).order());
    }
    
    return false;
  }
}
