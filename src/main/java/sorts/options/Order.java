package sorts.options;

import com.google.common.base.Preconditions;

public enum Order {
  ASCENDING,
  DESCENDING;

  public static final String FORWARD = "f";
  public static final String REVERSE = "r";
  
  public static String direction(Order o) { 
    Preconditions.checkNotNull(o);
    
    return Order.ASCENDING.equals(o) ? FORWARD : REVERSE;
  }
}
