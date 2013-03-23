import com.google.common.base.Preconditions;


public class Ordering {
  private final Order order;
  private final Column column;
  
  public Ordering(Column column) {
    this(column, Order.ASCENDING);
  }
  
  public Ordering(Column column, Order order) {
    Preconditions.checkNotNull(column);
    Preconditions.checkNotNull(order);
    
    this.column = column;
    this.order = order;
  }
  
  public Order order() {
    return this.order;
  }
  
  public Column column() {
    return this.column;
  }
  
  public static Ordering create(Column column) {
    return new Ordering(column);
  }
  
  public static Ordering create(Column column, Order order) {
    return new Ordering(column, order);
  }
}
