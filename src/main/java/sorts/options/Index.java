package sorts.options;

import com.google.common.base.Preconditions;

public class Index {
  
  protected final String column;
  protected final Order order;
  
  public Index(String column) {
    this(column, Order.ASCENDING);
  }
  
  public Index(String column, Order order) {
    Preconditions.checkNotNull(column);
    Preconditions.checkNotNull(order);
    
    this.column = column;
    this.order = order;
  }
  
  public static Index define(String column) {
    return new Index(column);
  }
  
  public static Index define(String column, Order order) {
    return new Index(column, order);
  }

  public String column() {
    return this.column;
  }

  public Order order() {
    return this.order;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Index) {
      Index other = (Index) o;
      
      if (this.column.equals(other.column) && this.order.equals(other.order)) {
        return true;
      }
    }
    
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.column.hashCode() ^ this.order.hashCode();
  }
  
}
