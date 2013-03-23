import com.google.common.base.Preconditions;


public class Ordering {
  private final Order order;
  private final String column;
  
  public Ordering(String column) {
    this(column, Order.ASCENDING);
  }
  
  public Ordering(String column, Order order) {
    Preconditions.checkNotNull(column);
    Preconditions.checkNotNull(order);
    
    this.column = column;
    this.order = order;
  }
  
  public Order order() {
    return this.order;
  }
  
  public String column() {
    return this.column;
  }
  
  public static Ordering create(String column) {
    return new Ordering(column);
  }
  
  public static Ordering create(String column, Order order) {
    return new Ordering(column, order);
  }
}
