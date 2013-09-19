package cosmos.util;

import cosmos.options.Index;
import cosmos.options.Order;

public class AscendingIndexIdentitySet extends IdentitySet<Index> {
  
  private static final AscendingIndexIdentitySet INSTANCE = new AscendingIndexIdentitySet();
  
  @SuppressWarnings("unchecked")
  public static AscendingIndexIdentitySet create() {
    return INSTANCE;
  }
  
  @Override
  public boolean contains(Object o) {
    if (o instanceof Index) {
      return Order.ASCENDING.equals(((Index) o).order());
    }
    
    return false;
  }
}
