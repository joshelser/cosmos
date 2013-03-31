package sorts.results;

import com.google.common.base.Preconditions;

public class Column {
  private final String column;
  
  public Column(String column) {
    this.column = column;
  }

  public String column() {
    return this.column;
  }
  
  public static Column create(String column) {
    Preconditions.checkNotNull(column);
    
    return new Column(column);
  }
  
  @Override
  public int hashCode() {
    return this.column.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof Column) {
      return this.column.equals(((Column) o).column());
    }
    
    return false;
  }
  
  @Override
  public String toString() {
    return column.toString();
  }
}
