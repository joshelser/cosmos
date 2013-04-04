package sorts.options;

import sorts.results.Column;

import com.google.common.base.Preconditions;

public class Index {
  
  protected final Column column;
  protected final Order order;
  
  public Index(Column column) {
    this(column, Order.ASCENDING);
  }
  
  public Index(Column column, Order order) {
    Preconditions.checkNotNull(column);
    Preconditions.checkNotNull(order);
    
    this.column = column;
    this.order = order;
  }
  
  public static Index define(String columnName) {
    return define(Column.create(columnName));
  }
  
  public static Index define(Column column) {
    return new Index(column);
  }
  
  public static Index define(Column column, Order order) {
    return new Index(column, order);
  }

  public Column column() {
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
